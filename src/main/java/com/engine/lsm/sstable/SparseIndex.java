package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * In-memory sparse index mapping sample keys to exact on-disk byte offsets.
 * Enables O(log M) binary search to bound disk seeks to small contiguous block ranges.
 */
public final class SparseIndex {
    public record SeekRange(long startOffset, long endOffset) {}

    private final List<IndexEntry> entries;

    public SparseIndex() {
        this.entries = new ArrayList<>();
    }

    public SparseIndex(List<IndexEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    public void add(ByteKey key, long offset) {
        entries.add(new IndexEntry(key, offset));
    }

    public List<IndexEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }

    public int size() {
        return entries.size();
    }

    /**
     * Performs binary search to locate the bounded byte offset range [startOffset, endOffset]
     * within the SSTable data block where the target key would reside.
     *
     * @param targetKey key being queried
     * @param dataBlockEndOffset end offset of the data block (where index block starts)
     * @return bounded seek range for disk scan
     */
    public SeekRange findSeekRange(ByteKey targetKey, long dataBlockEndOffset) {
        if (entries.isEmpty()) {
            return new SeekRange(0, dataBlockEndOffset);
        }

        int index = Collections.binarySearch(entries, new IndexEntry(targetKey, 0));

        if (index >= 0) {
            // Exact match on a sampled index key
            long start = entries.get(index).getOffset();
            long end = (index + 1 < entries.size()) ? entries.get(index + 1).getOffset() : dataBlockEndOffset;
            return new SeekRange(start, end);
        }

        int insertionPoint = -index - 1;

        if (insertionPoint == 0) {
            // Target key is smaller than the first sampled index key
            return new SeekRange(0, entries.get(0).getOffset());
        }

        if (insertionPoint >= entries.size()) {
            // Target key is greater than the last sampled index key
            return new SeekRange(entries.get(entries.size() - 1).getOffset(), dataBlockEndOffset);
        }

        // Target key is strictly bounded between [insertionPoint - 1] and [insertionPoint]
        long start = entries.get(insertionPoint - 1).getOffset();
        long end = entries.get(insertionPoint).getOffset();
        return new SeekRange(start, end);
    }

    /**
     * Serializes this sparse index into a ByteBuffer.
     * Format:
     * [EntryCount: 4B]
     * For each entry: [KeyLen: 4B][KeyBytes: K][Offset: 8B]
     */
    public ByteBuffer serialize() {
        int totalSize = 4;
        for (IndexEntry entry : entries) {
            totalSize += 4 + entry.getKey().length() + 8;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(entries.size());
        for (IndexEntry entry : entries) {
            byte[] keyBytes = entry.getKey().getBytes();
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.putLong(entry.getOffset());
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Deserializes a sparse index from a ByteBuffer.
     */
    public static SparseIndex deserialize(ByteBuffer buffer) {
        int count = buffer.getInt();
        List<IndexEntry> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int keyLen = buffer.getInt();
            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);
            long offset = buffer.getLong();
            list.add(new IndexEntry(ByteKey.of(keyBytes), offset));
        }
        return new SparseIndex(list);
    }
}
