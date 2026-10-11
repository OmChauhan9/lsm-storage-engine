package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;
import com.engine.lsm.wal.WalRecord;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Handles binary queries and scans against an immutable on-disk SSTable.
 * Keeps Bloom Filter and Sparse Index pinned in RAM for high-throughput reads.
 */
public class SSTableReader implements Closeable, Comparable<SSTableReader> {
    private final Path tablePath;
    private final long tableId;
    private final FileChannel channel;
    private final SSTableFooter footer;
    private final SparseIndex sparseIndex;
    private final BloomFilter bloomFilter;
    private final long fileSize;

    // Operational metrics
    private final AtomicLong bloomNegativeChecks = new AtomicLong(0);
    private final AtomicLong diskBlockSeeks = new AtomicLong(0);

    private SSTableReader(
            Path tablePath,
            long tableId,
            FileChannel channel,
            SSTableFooter footer,
            SparseIndex sparseIndex,
            BloomFilter bloomFilter,
            long fileSize
    ) {
        this.tablePath = tablePath;
        this.tableId = tableId;
        this.channel = channel;
        this.footer = footer;
        this.sparseIndex = sparseIndex;
        this.bloomFilter = bloomFilter;
        this.fileSize = fileSize;
    }

    /**
     * Opens an existing SSTable file, loads its footer, sparse index, and Bloom filter into RAM.
     */
    public static SSTableReader open(Path tablePath, long tableId) throws IOException {
        FileChannel channel = FileChannel.open(tablePath, StandardOpenOption.READ);
        long size = channel.size();
        if (size < SSTableFooter.FOOTER_SIZE) {
            channel.close();
            throw new IllegalStateException("Corrupt SSTable file: size less than footer minimum (" + size + " bytes)");
        }

        // 1. Read Footer from last 32 bytes
        ByteBuffer footerBuf = ByteBuffer.allocate(SSTableFooter.FOOTER_SIZE);
        channel.position(size - SSTableFooter.FOOTER_SIZE);
        channel.read(footerBuf);
        footerBuf.flip();
        SSTableFooter footer = SSTableFooter.deserialize(footerBuf);

        // 2. Read Sparse Index Block
        ByteBuffer indexBuf = ByteBuffer.allocate(footer.indexLength());
        channel.position(footer.indexOffset());
        channel.read(indexBuf);
        indexBuf.flip();
        SparseIndex sparseIndex = SparseIndex.deserialize(indexBuf);

        // 3. Read Bloom Filter Block
        ByteBuffer bloomBuf = ByteBuffer.allocate(footer.bloomFilterLength());
        channel.position(footer.bloomFilterOffset());
        channel.read(bloomBuf);
        bloomBuf.flip();
        BloomFilter bloomFilter = BloomFilter.deserialize(bloomBuf);

        return new SSTableReader(tablePath, tableId, channel, footer, sparseIndex, bloomFilter, size);
    }

    /**
     * Point lookup: Queries for the key within this SSTable.
     * <ol>
     *   <li>Checks in-memory Bloom filter (0 disk I/O if absent).</li>
     *   <li>Binary searches in-memory Sparse Index to find exact bounded block range.</li>
     *   <li>Reads small contiguous data slice from disk and scans sorted records.</li>
     * </ol>
     */
    public Record get(ByteKey key) throws IOException {
        // Step 1: Bloom Filter check
        if (!bloomFilter.mightContain(key)) {
            bloomNegativeChecks.incrementAndGet();
            return null; // Definitive negative: 0 disk reads!
        }

        diskBlockSeeks.incrementAndGet();

        // Step 2: Sparse Index Binary Search
        SparseIndex.SeekRange range = sparseIndex.findSeekRange(key, footer.indexOffset());
        long start = range.startOffset();
        long end = range.endOffset();
        int length = (int) (end - start);

        if (length <= 0) {
            return null;
        }

        // Step 3: Single contiguous positional disk read into memory buffer (thread-safe)
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int totalRead = channel.read(buffer, start);
        if (totalRead <= 0) {
            return null;
        }
        buffer.flip();

        CRC32 crc = new CRC32();
        byte[] targetKeyBytes = key.getBytes();

        while (buffer.remaining() >= WalRecord.HEADER_METADATA_SIZE) {
            int recordStartPos = buffer.position();
            int storedCrc = buffer.getInt();
            byte typeCode = buffer.get();
            int keyLen = buffer.getInt();
            int valLen = buffer.getInt();

            int payloadSize = keyLen + valLen;
            if (buffer.remaining() < payloadSize) {
                break;
            }

            byte[] keyBytes = new byte[keyLen];
            buffer.get(keyBytes);

            int cmp = Arrays.compareUnsigned(keyBytes, targetKeyBytes);
            if (cmp == 0) {
                // Key matches! Read value and verify CRC
                byte[] valBytes = new byte[valLen];
                buffer.get(valBytes);

                crc.reset();
                crc.update(typeCode);
                ByteBuffer lenBuf = ByteBuffer.allocate(8);
                lenBuf.putInt(keyLen);
                lenBuf.putInt(valLen);
                crc.update(lenBuf.array(), 0, 8);
                crc.update(keyBytes, 0, keyLen);
                crc.update(valBytes, 0, valLen);

                if ((int) crc.getValue() != storedCrc) {
                    throw new IllegalStateException("Data corruption: CRC mismatch for key " + key + " in " + tablePath);
                }

                RecordType type = RecordType.fromCode(typeCode);
                return (type == RecordType.PUT)
                        ? Record.put(key, valBytes, 0)
                        : Record.delete(key, 0);
            } else if (cmp > 0) {
                // Since records in SSTable are strictly sorted, exceeding target key means key does not exist
                return null;
            } else {
                // Skip value bytes and continue sequential scan
                buffer.position(buffer.position() + valLen);
            }
        }

        return null;
    }

    /**
     * Sequential iterator over the entire data block (used for compaction and range queries).
     */
    public Iterator<Map.Entry<ByteKey, Record>> iterator() {
        return new SSTableIterator(channel, 0, footer.indexOffset());
    }

    public long getTableId() {
        return tableId;
    }

    public Path getTablePath() {
        return tablePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public SparseIndex getSparseIndex() {
        return sparseIndex;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public long getBloomNegativeChecks() {
        return bloomNegativeChecks.get();
    }

    public long getDiskBlockSeeks() {
        return diskBlockSeeks.get();
    }

    @Override
    public int compareTo(SSTableReader other) {
        // Order by tableId descending (newest SSTables first in read path)
        return Long.compare(other.tableId, this.tableId);
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            channel.close();
        }
    }

    public synchronized void delete() throws IOException {
        close();
        Files.deleteIfExists(tablePath);
    }

    @Override
    public String toString() {
        return "SSTableReader{id=" + tableId + ", path=" + tablePath.getFileName() + ", size=" + fileSize + '}';
    }
}
