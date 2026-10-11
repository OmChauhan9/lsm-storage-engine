package com.engine.lsm.compaction;

import com.engine.lsm.sstable.SSTableReader;

import java.util.List;

/**
 * Encapsulates the outcome of an SSTable compaction cycle.
 */
public record CompactionResult(
        SSTableReader compactedReader,
        List<SSTableReader> obsoleteReaders,
        long bytesReclaimed,
        int keysDeduplicated,
        int tombstonesPurged
) {}
