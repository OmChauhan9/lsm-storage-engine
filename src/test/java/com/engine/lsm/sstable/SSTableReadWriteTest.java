package com.engine.lsm.sstable;

import com.engine.lsm.memtable.MemTable;
import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SSTableReadWriteTest {

    @Test
    @DisplayName("SSTableWriter and SSTableReader roundtrip with sparse index seeks and bloom suppression")
    void testWriteAndReadSSTable(@TempDir Path tempDir) throws IOException {
        Path sstablePath = tempDir.resolve("sstable-0000000001.db");
        int count = 1000;
        int stride = 16;

        MemTable memTable = new MemTable(1);
        for (int i = 0; i < count; i++) {
            // Use 5-digit padded string so lexicographical order matches numerical order
            ByteKey key = ByteKey.of("key-" + String.format("%05d", i));
            byte[] val = ("payload-value-" + i).getBytes();
            memTable.put(key, val, i + 1);
        }

        // Flush MemTable to SSTable file
        long bytesWritten = SSTableWriter.writeFromMemTable(sstablePath, memTable, stride, 0.01);
        assertTrue(bytesWritten > 0);

        // Open SSTableReader
        try (SSTableReader reader = SSTableReader.open(sstablePath, 1)) {
            assertEquals(1, reader.getTableId());
            assertTrue(reader.getSparseIndex().size() > 0);

            // 1. Point lookup for existing keys
            for (int i = 0; i < count; i += 13) { // Sample every 13th key
                ByteKey key = ByteKey.of("key-" + String.format("%05d", i));
                Record record = reader.get(key);
                assertNotNull(record, "Key " + key + " must be found in SSTable");
                assertArrayEquals(("payload-value-" + i).getBytes(), record.getValue());
            }

            // 2. Point lookup for absent keys should be suppressed by Bloom Filter
            for (int i = 0; i < 100; i++) {
                ByteKey absentKey = ByteKey.of("nonexistent-key-prefix-" + i);
                Record record = reader.get(absentKey);
                assertNull(record);
            }
            assertTrue(reader.getBloomNegativeChecks() > 90, "Bloom filter should suppress >90% of absent lookups without disk seek");

            // 3. Verify sequential streaming iterator order
            Iterator<Map.Entry<ByteKey, Record>> it = reader.iterator();
            int iteratedCount = 0;
            ByteKey prevKey = null;

            while (it.hasNext()) {
                Map.Entry<ByteKey, Record> entry = it.next();
                if (prevKey != null) {
                    assertTrue(prevKey.compareTo(entry.getKey()) < 0, "Keys must be strictly in ascending order");
                }
                prevKey = entry.getKey();
                iteratedCount++;
            }
            assertEquals(count, iteratedCount, "Iterator must stream all 1,000 records");
        }
    }
}
