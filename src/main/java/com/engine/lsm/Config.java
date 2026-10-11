package com.engine.lsm;

import java.nio.file.Path;

/**
 * Configuration parameters for the LSM Storage Engine.
 */
public final class Config {
    // Default threshold: 4 MB for MemTable before freezing and flushing
    public static final long DEFAULT_MEMTABLE_THRESHOLD_BYTES = 4 * 1024 * 1024;
    // Default sparse index stride: sample 1 key every 64 records
    public static final int DEFAULT_INDEX_STRIDE = 64;
    // Default false positive probability for Bloom filter: 1%
    public static final double DEFAULT_BLOOM_FP_RATE = 0.01;
    // Default compaction threshold: trigger compaction when SSTable count >= 4
    public static final int DEFAULT_COMPACTION_THRESHOLD = 4;

    private final Path dataDir;
    private final long memTableThresholdBytes;
    private final int indexStride;
    private final double bloomFilterFpRate;
    private final boolean syncWalOnWrite;
    private final int compactionThreshold;

    private Config(Builder builder) {
        this.dataDir = builder.dataDir;
        this.memTableThresholdBytes = builder.memTableThresholdBytes;
        this.indexStride = builder.indexStride;
        this.bloomFilterFpRate = builder.bloomFilterFpRate;
        this.syncWalOnWrite = builder.syncWalOnWrite;
        this.compactionThreshold = builder.compactionThreshold;
    }

    public static Builder builder(Path dataDir) {
        return new Builder(dataDir);
    }

    public static Config defaultConfig(Path dataDir) {
        return builder(dataDir).build();
    }

    public Path getDataDir() {
        return dataDir;
    }

    public Path getWalDir() {
        return dataDir.resolve("wal");
    }

    public Path getSstableDir() {
        return dataDir.resolve("sstable");
    }

    public long getMemTableThresholdBytes() {
        return memTableThresholdBytes;
    }

    public int getIndexStride() {
        return indexStride;
    }

    public double getBloomFilterFpRate() {
        return bloomFilterFpRate;
    }

    public boolean isSyncWalOnWrite() {
        return syncWalOnWrite;
    }

    public int getCompactionThreshold() {
        return compactionThreshold;
    }

    public static class Builder {
        private final Path dataDir;
        private long memTableThresholdBytes = DEFAULT_MEMTABLE_THRESHOLD_BYTES;
        private int indexStride = DEFAULT_INDEX_STRIDE;
        private double bloomFilterFpRate = DEFAULT_BLOOM_FP_RATE;
        private boolean syncWalOnWrite = false;
        private int compactionThreshold = DEFAULT_COMPACTION_THRESHOLD;

        public Builder(Path dataDir) {
            if (dataDir == null) {
                throw new IllegalArgumentException("Data directory cannot be null");
            }
            this.dataDir = dataDir;
        }

        public Builder memTableThresholdBytes(long bytes) {
            if (bytes <= 0) throw new IllegalArgumentException("Threshold must be positive");
            this.memTableThresholdBytes = bytes;
            return this;
        }

        public Builder indexStride(int stride) {
            if (stride <= 0) throw new IllegalArgumentException("Index stride must be positive");
            this.indexStride = stride;
            return this;
        }

        public Builder bloomFilterFpRate(double rate) {
            if (rate <= 0 || rate >= 1.0) throw new IllegalArgumentException("Bloom FP rate must be in (0, 1)");
            this.bloomFilterFpRate = rate;
            return this;
        }

        public Builder syncWalOnWrite(boolean sync) {
            this.syncWalOnWrite = sync;
            return this;
        }

        public Builder compactionThreshold(int threshold) {
            if (threshold < 2) throw new IllegalArgumentException("Compaction threshold must be >= 2");
            this.compactionThreshold = threshold;
            return this;
        }

        public Config build() {
            return new Config(this);
        }
    }
}
