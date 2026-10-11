package com.engine.lsm.memtable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe memory tracker for atomic byte-level accounting of in-memory structures.
 */
public final class MemoryTracker {
    private final AtomicLong allocatedBytes = new AtomicLong(0);

    public MemoryTracker() {}

    public MemoryTracker(long initialBytes) {
        this.allocatedBytes.set(initialBytes);
    }

    public long add(long bytes) {
        return allocatedBytes.addAndGet(bytes);
    }

    public long subtract(long bytes) {
        return allocatedBytes.addAndGet(-bytes);
    }

    public long get() {
        return allocatedBytes.get();
    }

    public void reset() {
        allocatedBytes.set(0);
    }

    @Override
    public String toString() {
        return "MemoryTracker{" + allocatedBytes.get() + " bytes}";
    }
}
