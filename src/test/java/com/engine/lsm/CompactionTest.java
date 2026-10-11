package com.engine.lsm;

import com.engine.lsm.compaction.CompactionResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CompactionTest {

    @Test
    @DisplayName("2-Way Compaction merges SSTables, deduplicates keys, purges tombstones, and reclaims disk space")
    void testCompactionEndToEnd(@TempDir Path tempDir) throws IOException {
        Config config = Config.builder(tempDir)
                .memTableThresholdBytes(1024 * 1024)
                .compactionThreshold(10) // don't auto-compact during test setup
                .build();

        try (LsmEngine engine = LsmEngine.open(config)) {
            // --- Batch 1 ---
            engine.put("k1", "v1_old");
            engine.put("k2", "v2_initial");
            engine.put("k3", "v3_stable");
            engine.flush(); // Creates SSTable 1

            // --- Batch 2 ---
            engine.put("k1", "v1_latest"); // Overwrite
            engine.delete("k2");           // Tombstone deletion
            engine.put("k4", "v4_new");
            engine.flush(); // Creates SSTable 2

            assertEquals(2, engine.getSstables().size(), "Must have exactly 2 SSTables before compaction");

            // Verify read state before compaction
            assertEquals("v1_latest", engine.get("k1"));
            assertNull(engine.get("k2"));
            assertEquals("v3_stable", engine.get("k3"));
            assertEquals("v4_new", engine.get("k4"));

            // Execute Compaction
            CompactionResult result = engine.compact();

            assertNotNull(result.compactedReader());
            assertEquals(1, engine.getSstables().size(), "Must have exactly 1 SSTable after compaction");
            assertTrue(result.keysDeduplicated() >= 1, "At least key 'k1' must have been deduplicated");
            assertTrue(result.tombstonesPurged() >= 1, "Tombstone for 'k2' must have been purged at bottom tier");

            // Verify read state after compaction
            assertEquals("v1_latest", engine.get("k1"), "Latest value must be preserved");
            assertNull(engine.get("k2"), "Deleted key must remain null");
            assertEquals("v3_stable", engine.get("k3"));
            assertEquals("v4_new", engine.get("k4"));
        }
    }
}
