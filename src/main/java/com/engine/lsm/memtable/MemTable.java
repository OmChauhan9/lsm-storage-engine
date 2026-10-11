package com.engine.lsm.memtable;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Thread-safe, lock-free in-memory buffer backed by {@link ConcurrentSkipListMap}.
 * Maintains sorted order of keys and tracks memory consumption dynamically.
 */
public class MemTable implements Iterable<Map.Entry<ByteKey, Record>> {
    private final ConcurrentSkipListMap<ByteKey, Record> map = new ConcurrentSkipListMap<>();
    private final MemoryTracker memoryTracker = new MemoryTracker();
    private final long walSegmentId;
    private volatile boolean frozen = false;

    public MemTable(long walSegmentId) {
        this.walSegmentId = walSegmentId;
    }

    public long getWalSegmentId() {
        return walSegmentId;
    }

    /**
     * Inserts or updates a key-value record in the MemTable.
     */
    public void put(ByteKey key, byte[] value, long sequenceNumber) {
        ensureNotFrozen();
        Record newRecord = Record.put(key, value, sequenceNumber);
        Record oldRecord = map.put(key, newRecord);

        long netMemoryDelta = newRecord.estimatedHeapSize();
        if (oldRecord != null) {
            netMemoryDelta -= oldRecord.estimatedHeapSize();
        }
        memoryTracker.add(netMemoryDelta);
    }

    /**
     * Inserts a deletion tombstone for the given key.
     */
    public void delete(ByteKey key, long sequenceNumber) {
        ensureNotFrozen();
        Record tombstone = Record.delete(key, sequenceNumber);
        Record oldRecord = map.put(key, tombstone);

        long netMemoryDelta = tombstone.estimatedHeapSize();
        if (oldRecord != null) {
            netMemoryDelta -= oldRecord.estimatedHeapSize();
        }
        memoryTracker.add(netMemoryDelta);
    }

    /**
     * Retrieves the record for the given key, or null if not present.
     */
    public Record get(ByteKey key) {
        return map.get(key);
    }

    public long getByteSize() {
        return memoryTracker.get();
    }

    public int entryCount() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean isFrozen() {
        return frozen;
    }

    public synchronized void freeze() {
        this.frozen = true;
    }

    private void ensureNotFrozen() {
        if (frozen) {
            throw new IllegalStateException("MemTable is frozen and cannot accept new writes");
        }
    }

    @Override
    public Iterator<Map.Entry<ByteKey, Record>> iterator() {
        return Collections.unmodifiableMap(map).entrySet().iterator();
    }

    public Map<ByteKey, Record> asMap() {
        return Collections.unmodifiableMap(map);
    }
}
