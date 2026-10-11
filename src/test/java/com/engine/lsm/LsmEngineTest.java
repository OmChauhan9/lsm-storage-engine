package com.engine.lsm;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LsmEngineTest {

    @Test
    @DisplayName("Basic Put, Get, Update, and Delete operations")
    void testBasicCrud(@TempDir Path tempDir) throws IOException {
        try (LsmEngine engine = LsmEngine.open(tempDir)) {
            // Put and Get
            engine.put("user:101", "Alice");
            assertEquals("Alice", engine.get("user:101"));

            // Update
            engine.put("user:101", "Alice Smith");
            assertEquals("Alice Smith", engine.get("user:101"));

            // Non-existent key
            assertNull(engine.get("user:999"));

            // Delete
            engine.delete("user:101");
            assertNull(engine.get("user:101"));
        }
    }

    @Test
    @DisplayName("Tiered reads across MemTable and multiple on-disk SSTables")
    void testTieredReadsAcrossSstables(@TempDir Path tempDir) throws IOException {
        Config config = Config.builder(tempDir)
                .memTableThresholdBytes(2048) // Small threshold to trigger flushes quickly
                .indexStride(4)
                .build();

        try (LsmEngine engine = LsmEngine.open(config)) {
            // Write 100 entries to force multiple SSTable flushes
            for (int i = 0; i < 100; i++) {
                engine.put("key:" + String.format("%03d", i), "value-" + i);
            }
            engine.flush();

            assertTrue(engine.getSstables().size() >= 1, "Should have created on-disk SSTables");

            // Overwrite key:005 in MemTable
            engine.put("key:005", "updated-value-in-memtable");

            // Read path should find key:005 in active MemTable before checking SSTables
            assertEquals("updated-value-in-memtable", engine.get("key:005"));

            // Delete key:010 in MemTable
            engine.delete("key:010");
            // Tombstone in MemTable must hide older value stored in SSTable
            assertNull(engine.get("key:010"));

            // Read unmodified key from SSTable
            assertEquals("value-50", engine.get("key:050"));
        }
    }

    @Test
    @DisplayName("Persistence across engine restarts (Clean shutdown)")
    void testCleanShutdownPersistence(@TempDir Path tempDir) throws IOException {
        // Step 1: Open engine, write data, close
        try (LsmEngine engine = LsmEngine.open(tempDir)) {
            for (int i = 0; i < 50; i++) {
                engine.put("sensor:" + i, "temp=" + (20 + i));
            }
        } // close() triggers flush and saves active data

        // Step 2: Re-open engine from same directory
        try (LsmEngine engine = LsmEngine.open(tempDir)) {
            for (int i = 0; i < 50; i++) {
                assertEquals("temp=" + (20 + i), engine.get("sensor:" + i));
            }
        }
    }
}
