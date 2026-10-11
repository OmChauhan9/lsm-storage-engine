package com.engine.lsm.sstable;

import java.nio.ByteBuffer;

/**
 * Fixed-size 32-byte metadata footer appended to every SSTable file.
 * Enables O(1) discovery of Sparse Index and Bloom Filter offsets without full file scans.
 * <p>
 * Layout:
 * [IndexOffset: 8B][IndexLength: 4B][BloomFilterOffset: 8B][BloomFilterLength: 4B][MagicNumber: 8B]
 */
public record SSTableFooter(
        long indexOffset,
        int indexLength,
        long bloomFilterOffset,
        int bloomFilterLength
) {
    public static final long MAGIC_NUMBER = 0x4C534D5452454531L; // "LSMTREE1" in ASCII-hex
    public static final int FOOTER_SIZE = 32; // 8 + 4 + 8 + 4 + 8

    public ByteBuffer serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(FOOTER_SIZE);
        buffer.putLong(indexOffset);
        buffer.putInt(indexLength);
        buffer.putLong(bloomFilterOffset);
        buffer.putInt(bloomFilterLength);
        buffer.putLong(MAGIC_NUMBER);
        buffer.flip();
        return buffer;
    }

    public static SSTableFooter deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < FOOTER_SIZE) {
            throw new IllegalArgumentException("Insufficient bytes for SSTable footer. Expected " + FOOTER_SIZE);
        }
        long indexOffset = buffer.getLong();
        int indexLength = buffer.getInt();
        long bloomFilterOffset = buffer.getLong();
        int bloomFilterLength = buffer.getInt();
        long magic = buffer.getLong();

        if (magic != MAGIC_NUMBER) {
            throw new IllegalStateException("Corrupt SSTable footer: magic number mismatch 0x" + Long.toHexString(magic));
        }

        return new SSTableFooter(indexOffset, indexLength, bloomFilterOffset, bloomFilterLength);
    }
}
