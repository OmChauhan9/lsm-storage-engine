package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;

import java.util.Objects;

/**
 * An individual entry in the SSTable sparse index.
 */
public final class IndexEntry implements Comparable<IndexEntry> {
    private final ByteKey key;
    private final long offset;

    public IndexEntry(ByteKey key, long offset) {
        this.key = Objects.requireNonNull(key, "Index key cannot be null");
        this.offset = offset;
    }

    public ByteKey getKey() {
        return key;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public int compareTo(IndexEntry other) {
        return this.key.compareTo(other.key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexEntry that = (IndexEntry) o;
        return offset == that.offset && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, offset);
    }

    @Override
    public String toString() {
        return "IndexEntry{" + key + " -> @" + offset + '}';
    }
}
