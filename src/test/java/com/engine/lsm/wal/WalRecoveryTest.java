package com.engine.lsm.wal;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class WalRecoveryTest {

    @Test
    @DisplayName("Clean recovery restores all records and updates sequence numbers")
    void testCleanRecovery(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("wal-clean.log");
        int count = 200;

        try (WriteAheadLog wal = new WriteAheadLog(walPath, false)) {
            for (int i = 0; i < count; i++) {
                if (i % 2 == 0) {
                    wal.appendPut(ByteKey.of("k" + i), ("v" + i).getBytes());
                } else {
                    wal.appendDelete(ByteKey.of("k" + i));
                }
            }
        }

        AtomicLong seqGen = new AtomicLong(0);
        WalRecovery.RecoveryResult result = WalRecovery.recover(walPath, seqGen);

        assertEquals(count, result.records().size());
        assertEquals(count, result.maxSequenceNumber());

        for (int i = 0; i < count; i++) {
            Record r = result.records().get(i);
            assertEquals(ByteKey.of("k" + i), r.getKey());
            if (i % 2 == 0) {
                assertEquals(RecordType.PUT, r.getType());
                assertArrayEquals(("v" + i).getBytes(), r.getValue());
            } else {
                assertEquals(RecordType.DELETE, r.getType());
                assertTrue(r.isTombstone());
            }
        }
    }

    @Test
    @DisplayName("Crash recovery handles torn frame at tail and truncates safely")
    void testTornFrameRecovery(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("wal-torn.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath, false)) {
            for (int i = 0; i < 50; i++) {
                wal.appendPut(ByteKey.of("key-" + i), ("value-" + i).getBytes());
            }
        }

        long cleanSize = Files.size(walPath);

        // Artificially append 7 garbage bytes simulating a mid-write crash/power loss
        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer garbage = ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7});
            channel.write(garbage);
        }

        assertTrue(Files.size(walPath) > cleanSize);

        AtomicLong seqGen = new AtomicLong(0);
        WalRecovery.RecoveryResult result = WalRecovery.recover(walPath, seqGen);

        assertEquals(50, result.records().size(), "Should recover all 50 clean records");
        assertEquals(cleanSize, Files.size(walPath), "File should be truncated back to clean byte boundary");
    }

    @Test
    @DisplayName("Crash recovery detects bit-flip / CRC corruption and truncates damaged tail")
    void testCrcMismatchRecovery(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("wal-corrupt.log");

        try (WriteAheadLog wal = new WriteAheadLog(walPath, false)) {
            for (int i = 0; i < 10; i++) {
                wal.appendPut(ByteKey.of("key-" + i), ("value-" + i).getBytes());
            }
        }

        // Corrupt a byte in the 10th record
        long fileSize = Files.size(walPath);
        try (FileChannel channel = FileChannel.open(walPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer flip = ByteBuffer.allocate(1);
            flip.put((byte) 0xFF);
            flip.flip();
            // Corrupt the byte near the tail
            channel.position(fileSize - 2);
            channel.write(flip);
        }

        AtomicLong seqGen = new AtomicLong(0);
        WalRecovery.RecoveryResult result = WalRecovery.recover(walPath, seqGen);

        // First 9 records must be recovered cleanly; corrupted 10th record discarded
        assertEquals(9, result.records().size());
    }
}
