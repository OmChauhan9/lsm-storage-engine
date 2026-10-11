package com.engine.lsm;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class WalCrashRecoveryTest {

    @Test
    @DisplayName("Simulate mid-write power loss crash: unclosed WAL with torn tail recovers 100% committed records")
    void testCrashRecoveryWithTornTail(@TempDir Path tempDir) throws IOException {
        Config config = Config.builder(tempDir)
                .memTableThresholdBytes(1024 * 1024) // 1MB threshold so all writes stay in WAL/MemTable
                .syncWalOnWrite(false)
                .build();

        // 1. Open engine and write 500 records
        LsmEngine engine = LsmEngine.open(config);
        for (int i = 0; i < 500; i++) {
            engine.put("account:" + i, "balance=" + (1000 + i));
        }

        // Deliberately simulate hard crash WITHOUT calling engine.close()!
        // Find the active WAL file
        Path walFile = null;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(config.getWalDir(), "wal-*.log")) {
            for (Path p : stream) {
                walFile = p;
                break;
            }
        }
        assertNotNull(walFile, "Active WAL file must exist on disk");

        // 2. Corrupt the tail by appending 11 partial bytes simulating a power outage during a write
        try (FileChannel channel = FileChannel.open(walFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer partialCrash = ByteBuffer.wrap(new byte[]{0x12, 0x34, 0x56, 0x78, 0x01, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00});
            channel.write(partialCrash);
            channel.force(true);
        }

        // 3. Boot new LsmEngine instance from the same data directory
        try (LsmEngine recoveredEngine = LsmEngine.open(config)) {
            // Verify that all 500 committed records survived the crash and were recovered
            for (int i = 0; i < 500; i++) {
                assertEquals("balance=" + (1000 + i), recoveredEngine.get("account:" + i),
                        "Record account:" + i + " must be recovered after crash");
            }

            // Verify the engine is healthy and continues accepting new writes
            recoveredEngine.put("account:new", "balance=9999");
            assertEquals("balance=9999", recoveredEngine.get("account:new"));
        }
    }
}
