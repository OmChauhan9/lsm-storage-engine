package com.engine.lsm.wal;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Robust crash recovery parser.
 * <p>
 * Scans WAL segments from offset 0, validates CRC32 integrity of every frame,
 * gracefully handles mid-write torn frames caused by power-loss/process crash by
 * truncating the log to the last known valid boundary, and returns all committed records.
 */
public final class WalRecovery {

    public record RecoveryResult(List<Record> records, long lastValidOffset, long maxSequenceNumber) {}

    private WalRecovery() {}

    /**
     * Recovers all valid records from the specified WAL file.
     * If an uncorrupted partial record is found at the tail (from a mid-write crash),
     * truncates the file to the last valid byte offset.
     *
     * @param logPath path to the WAL file
     * @param sequenceGenerator atomic sequence generator to keep numbering synchronized
     * @return RecoveryResult containing recovered records and the truncated valid offset
     */
    public static RecoveryResult recover(Path logPath, AtomicLong sequenceGenerator) throws IOException {
        List<Record> records = new ArrayList<>();
        if (!java.nio.file.Files.exists(logPath)) {
            return new RecoveryResult(records, 0, sequenceGenerator.get());
        }

        long lastValidOffset = 0;
        CRC32 crc = new CRC32();

        try (FileChannel channel = FileChannel.open(logPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long fileSize = channel.size();
            if (fileSize == 0) {
                return new RecoveryResult(records, 0, sequenceGenerator.get());
            }

            ByteBuffer headerBuffer = ByteBuffer.allocate(WalRecord.HEADER_METADATA_SIZE);

            while (channel.position() < fileSize) {
                long recordStartOffset = channel.position();
                headerBuffer.clear();

                // Read 13-byte header: [CRC: 4B][Type: 1B][KeyLen: 4B][ValLen: 4B]
                int bytesRead = channel.read(headerBuffer);
                if (bytesRead < WalRecord.HEADER_METADATA_SIZE) {
                    // Torn frame at EOF
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }

                headerBuffer.flip();
                int storedCrc = headerBuffer.getInt();
                byte typeCode = headerBuffer.get();
                int keyLen = headerBuffer.getInt();
                int valLen = headerBuffer.getInt();

                // Sanity check lengths to avoid allocation attacks or corrupted lengths
                if (keyLen <= 0 || valLen < 0 || keyLen > 16 * 1024 * 1024 || valLen > 64 * 1024 * 1024) {
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }

                RecordType type;
                try {
                    type = RecordType.fromCode(typeCode);
                } catch (IllegalArgumentException e) {
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }

                int payloadWithoutHeader = keyLen + valLen;
                if (channel.position() + payloadWithoutHeader > fileSize) {
                    // Partial payload write before crash
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }

                byte[] keyBytes = new byte[keyLen];
                byte[] valBytes = new byte[valLen];

                ByteBuffer payloadBuf = ByteBuffer.allocate(payloadWithoutHeader);
                int payloadRead = channel.read(payloadBuf);
                if (payloadRead < payloadWithoutHeader) {
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }
                payloadBuf.flip();
                payloadBuf.get(keyBytes);
                payloadBuf.get(valBytes);

                // Recompute CRC32 over [Type: 1B][KeyLen: 4B][ValLen: 4B][Key][Val]
                crc.reset();
                crc.update(typeCode);
                ByteBuffer lenBuf = ByteBuffer.allocate(8);
                lenBuf.putInt(keyLen);
                lenBuf.putInt(valLen);
                crc.update(lenBuf.array(), 0, 8);
                crc.update(keyBytes, 0, keyLen);
                crc.update(valBytes, 0, valLen);

                int computedCrc = (int) crc.getValue();
                if (computedCrc != storedCrc) {
                    // CRC mismatch: data corruption
                    truncateAndExit(channel, recordStartOffset);
                    lastValidOffset = recordStartOffset;
                    break;
                }

                // Clean valid record
                long seq = sequenceGenerator.incrementAndGet();
                ByteKey key = ByteKey.of(keyBytes);
                Record record = (type == RecordType.PUT)
                        ? Record.put(key, valBytes, seq)
                        : Record.delete(key, seq);

                records.add(record);
                lastValidOffset = channel.position();
            }
        }

        return new RecoveryResult(records, lastValidOffset, sequenceGenerator.get());
    }

    private static void truncateAndExit(FileChannel channel, long validOffset) throws IOException {
        channel.truncate(validOffset);
        channel.force(true);
    }
}
