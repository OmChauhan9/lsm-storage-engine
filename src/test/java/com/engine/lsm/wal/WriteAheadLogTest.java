package com.engine.lsm.wal;

import com.engine.lsm.model.ByteKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {

    @Test
    @DisplayName("WAL file size on disk exactly matches sum of (13 + K + V) binary frames")
    void testBinaryFramingAndSize(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("test-wal.log");
        int count = 1000;
        long expectedBytes = 0;

        try (WriteAheadLog wal = new WriteAheadLog(walPath, false)) {
            for (int i = 0; i < count; i++) {
                ByteKey key = ByteKey.of("key-" + String.format("%05d", i));
                byte[] val = ("payload-value-data-" + i).getBytes();

                expectedBytes += 13 + key.length() + val.length;
                wal.appendPut(key, val);
            }
        }

        assertTrue(Files.exists(walPath));
        long actualSize = Files.size(walPath);
        assertEquals(expectedBytes, actualSize, "File size must exactly match formula sum(13 + K + V)");
    }

    @Test
    @DisplayName("WAL handles tombstone records with zero value length")
    void testTombstoneFraming(@TempDir Path tempDir) throws IOException {
        Path walPath = tempDir.resolve("tombstone.log");
        ByteKey key = ByteKey.of("deleted-key");
        long expectedBytes = 13 + key.length() + 0;

        try (WriteAheadLog wal = new WriteAheadLog(walPath, true)) {
            wal.appendDelete(key);
        }

        assertEquals(expectedBytes, Files.size(walPath));
    }
}
