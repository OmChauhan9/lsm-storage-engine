package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;
import com.engine.lsm.wal.WalRecord;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

/**
 * Sequential streaming iterator over an SSTable's data block.
 * Reads binary records between byte 0 and dataBlockEndOffset.
 */
public class SSTableIterator implements Iterator<Map.Entry<ByteKey, Record>>, Closeable {
    private final FileChannel channel;
    private final long endOffset;
    private long currentOffset;
    private Map.Entry<ByteKey, Record> nextEntry;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(WalRecord.HEADER_METADATA_SIZE);
    private final CRC32 crc = new CRC32();

    public SSTableIterator(FileChannel channel, long startOffset, long endOffset) {
        this.channel = channel;
        this.currentOffset = startOffset;
        this.endOffset = endOffset;
        advance();
    }

    private void advance() {
        nextEntry = null;
        if (currentOffset >= endOffset) {
            return;
        }

        try {
            headerBuffer.clear();
            int read = channel.read(headerBuffer, currentOffset);
            if (read < WalRecord.HEADER_METADATA_SIZE) {
                return;
            }

            headerBuffer.flip();
            int storedCrc = headerBuffer.getInt();
            byte typeCode = headerBuffer.get();
            int keyLen = headerBuffer.getInt();
            int valLen = headerBuffer.getInt();

            int payloadSize = keyLen + valLen;
            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize);
            int payloadRead = channel.read(payloadBuf, currentOffset + WalRecord.HEADER_METADATA_SIZE);
            if (payloadRead < payloadSize) {
                return;
            }
            payloadBuf.flip();

            byte[] keyBytes = new byte[keyLen];
            byte[] valBytes = new byte[valLen];
            payloadBuf.get(keyBytes);
            payloadBuf.get(valBytes);

            // Recompute CRC32
            crc.reset();
            crc.update(typeCode);
            ByteBuffer lenBuf = ByteBuffer.allocate(8);
            lenBuf.putInt(keyLen);
            lenBuf.putInt(valLen);
            crc.update(lenBuf.array(), 0, 8);
            crc.update(keyBytes, 0, keyLen);
            crc.update(valBytes, 0, valLen);

            if ((int) crc.getValue() != storedCrc) {
                throw new IllegalStateException("Corrupt SSTable record CRC at offset " + currentOffset);
            }

            ByteKey key = ByteKey.of(keyBytes);
            RecordType type = RecordType.fromCode(typeCode);
            Record record = (type == RecordType.PUT)
                    ? Record.put(key, valBytes, 0)
                    : Record.delete(key, 0);

            nextEntry = Map.entry(key, record);
            currentOffset += WalRecord.HEADER_METADATA_SIZE + payloadSize;
        } catch (IOException e) {
            throw new RuntimeException("Error reading SSTable stream at offset " + currentOffset, e);
        }
    }

    @Override
    public boolean hasNext() {
        return nextEntry != null;
    }

    @Override
    public Map.Entry<ByteKey, Record> next() {
        if (nextEntry == null) {
            throw new NoSuchElementException();
        }
        Map.Entry<ByteKey, Record> current = nextEntry;
        advance();
        return current;
    }

    @Override
    public void close() {
        // Does not close channel if reader owns it
    }
}
