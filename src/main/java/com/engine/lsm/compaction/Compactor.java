package com.engine.lsm.compaction;

import com.engine.lsm.Config;
import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.sstable.SSTableReader;
import com.engine.lsm.sstable.SSTableWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Executes streaming multi-way (k-way) merge-sort compaction across SSTables.
 * <p>
 * Eliminates duplicate key updates by keeping only the latest version,
 * purges obsolete tombstones, and reclaims disk space.
 */
public final class Compactor {

    private Compactor() {}

    private static final class IteratorHead implements Comparable<IteratorHead> {
        private final int readerIndex; // lower index = newer SSTable
        private final Iterator<Map.Entry<ByteKey, Record>> iterator;
        private Map.Entry<ByteKey, Record> currentEntry;

        public IteratorHead(int readerIndex, Iterator<Map.Entry<ByteKey, Record>> iterator) {
            this.readerIndex = readerIndex;
            this.iterator = iterator;
            if (iterator.hasNext()) {
                this.currentEntry = iterator.next();
            }
        }

        public boolean hasNext() {
            return currentEntry != null;
        }

        public void advance() {
            if (iterator.hasNext()) {
                currentEntry = iterator.next();
            } else {
                currentEntry = null;
            }
        }

        public ByteKey getKey() {
            return currentEntry.getKey();
        }

        public Record getRecord() {
            return currentEntry.getValue();
        }

        public Map.Entry<ByteKey, Record> getEntry() {
            return currentEntry;
        }

        @Override
        public int compareTo(IteratorHead other) {
            int cmp = this.getKey().compareTo(other.getKey());
            if (cmp != 0) {
                return cmp;
            }
            // For identical keys, the newer SSTable (lower readerIndex) has higher precedence
            return Integer.compare(this.readerIndex, other.readerIndex);
        }
    }

    /**
     * Compacts the specified list of SSTables into a single target SSTable.
     *
     * @param tablesToCompact list of SSTable readers ordered from newest to oldest
     * @param targetPath file path for the newly compacted SSTable
     * @param newTableId unique identifier for the new SSTable
     * @param isBottomTier true if no older SSTables exist below this set (safe to purge tombstones)
     * @param config engine configuration
     * @return CompactionResult containing the new reader and cleanup metrics
     */
    public static CompactionResult compact(
            List<SSTableReader> tablesToCompact,
            Path targetPath,
            long newTableId,
            boolean isBottomTier,
            Config config
    ) throws IOException {
        if (tablesToCompact.isEmpty()) {
            throw new IllegalArgumentException("Cannot compact empty SSTable list");
        }

        long oldTotalBytes = 0;
        PriorityQueue<IteratorHead> minHeap = new PriorityQueue<>();

        for (int i = 0; i < tablesToCompact.size(); i++) {
            SSTableReader reader = tablesToCompact.get(i);
            oldTotalBytes += reader.getFileSize();
            IteratorHead head = new IteratorHead(i, reader.iterator());
            if (head.hasNext()) {
                minHeap.add(head);
            }
        }

        List<Map.Entry<ByteKey, Record>> mergedEntries = new ArrayList<>();
        int deduplicatedKeys = 0;
        int tombstonesPurged = 0;

        while (!minHeap.isEmpty()) {
            IteratorHead top = minHeap.poll();
            ByteKey currentKey = top.getKey();
            Record latestRecord = top.getRecord();
            Map.Entry<ByteKey, Record> latestEntry = top.getEntry();

            // Advance the top iterator and re-add if not exhausted
            top.advance();
            if (top.hasNext()) {
                minHeap.add(top);
            }

            // Drain any other iterators in the heap that have the exact same key
            while (!minHeap.isEmpty() && minHeap.peek().getKey().equals(currentKey)) {
                IteratorHead duplicateHead = minHeap.poll();
                deduplicatedKeys++;
                duplicateHead.advance();
                if (duplicateHead.hasNext()) {
                    minHeap.add(duplicateHead);
                }
            }

            // Evaluate Tombstone
            if (latestRecord.isTombstone()) {
                if (isBottomTier) {
                    // Safe to completely drop tombstone: no older SSTables exist with this key!
                    tombstonesPurged++;
                    continue;
                }
            }

            mergedEntries.add(latestEntry);
        }

        // Write the merged entries to the new SSTable file
        SSTableWriter.writeFromIterator(
                targetPath,
                mergedEntries.size(),
                mergedEntries.iterator(),
                config.getIndexStride(),
                config.getBloomFilterFpRate()
        );

        SSTableReader newReader = SSTableReader.open(targetPath, newTableId);
        long newTotalBytes = newReader.getFileSize();
        long bytesReclaimed = Math.max(0, oldTotalBytes - newTotalBytes);

        return new CompactionResult(
                newReader,
                tablesToCompact,
                bytesReclaimed,
                deduplicatedKeys,
                tombstonesPurged
        );
    }
}
