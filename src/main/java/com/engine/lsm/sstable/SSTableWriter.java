package com.engine.lsm.sstable;

import com.engine.lsm.memtable.MemTable;
import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.wal.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;

/**
 * Sequential writer for creating immutable on-disk Sorted String Tables (SSTables).
 * Flushes sorted key-value pairs contiguously, constructs an in-memory Sparse Index,
 * computes a Bloom Filter, and writes a fixed 32-byte footer.
 */
public final class SSTableWriter {

    private SSTableWriter() {}

    /**
     * Flushes a frozen MemTable to a new SSTable file.
     *
     * @param tablePath target .db file path
     * @param memTable frozen MemTable containing sorted records
     * @param indexStride sample every N records into the sparse index
     * @param bloomFpRate target false positive rate for Bloom filter
     * @return total bytes written to the SSTable file
     */
    public static long writeFromMemTable(
            Path tablePath,
            MemTable memTable,
            int indexStride,
            double bloomFpRate
    ) throws IOException {
        return writeFromIterator(
                tablePath,
                memTable.entryCount(),
                memTable.iterator(),
                indexStride,
                bloomFpRate
        );
    }

    /**
     * Writes sorted records from an arbitrary iterator (used for MemTable flushes and Compactions).
     */
    public static long writeFromIterator(
            Path tablePath,
            int estimatedEntries,
            Iterator<Map.Entry<ByteKey, Record>> iterator,
            int indexStride,
            double bloomFpRate
    ) throws IOException {
        Path parent = tablePath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        SparseIndex sparseIndex = new SparseIndex();
        BloomFilter bloomFilter = new BloomFilter(Math.max(1, estimatedEntries), bloomFpRate);

        try (FileChannel channel = FileChannel.open(
                tablePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            long currentOffset = 0;
            int count = 0;

            // 1. Write Contiguous Sorted Data Block
            while (iterator.hasNext()) {
                Map.Entry<ByteKey, Record> entry = iterator.next();
                ByteKey key = entry.getKey();
                Record record = entry.getValue();

                // Sample every Nth entry into Sparse Index
                if (count % indexStride == 0) {
                    sparseIndex.add(key, currentOffset);
                }

                // Add to Bloom Filter
                bloomFilter.add(key);

                // Serialize binary frame
                ByteBuffer frame = WalRecord.serialize(record);
                int frameLength = frame.remaining();
                while (frame.hasRemaining()) {
                    channel.write(frame);
                }

                currentOffset += frameLength;
                count++;
            }

            // 2. Write Sparse Index Block
            long indexOffset = currentOffset;
            ByteBuffer indexBuf = sparseIndex.serialize();
            int indexLength = indexBuf.remaining();
            while (indexBuf.hasRemaining()) {
                channel.write(indexBuf);
            }
            currentOffset += indexLength;

            // 3. Write Bloom Filter Block
            long bloomFilterOffset = currentOffset;
            ByteBuffer bloomBuf = bloomFilter.serialize();
            int bloomFilterLength = bloomBuf.remaining();
            while (bloomBuf.hasRemaining()) {
                channel.write(bloomBuf);
            }
            currentOffset += bloomFilterLength;

            // 4. Write 32-Byte Fixed Footer
            SSTableFooter footer = new SSTableFooter(
                    indexOffset,
                    indexLength,
                    bloomFilterOffset,
                    bloomFilterLength
            );
            ByteBuffer footerBuf = footer.serialize();
            while (footerBuf.hasRemaining()) {
                channel.write(footerBuf);
            }
            currentOffset += SSTableFooter.FOOTER_SIZE;

            // Fsync to ensure complete durability of all blocks
            channel.force(true);
            return currentOffset;
        }
    }
}
