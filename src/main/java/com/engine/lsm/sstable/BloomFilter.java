package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * High-performance Bloom Filter utilizing the Kirsch-Mitzenmacher optimization
 * to simulate k independent hash functions using 64-bit Murmur3-like split hashes.
 * <p>
 * Sizing Formulas:
 * <ul>
 *   <li>m = - (n * ln(p)) / (ln(2)^2) [Total bit count]</li>
 *   <li>k = (m / n) * ln(2)          [Number of hash functions]</li>
 * </ul>
 */
public final class BloomFilter {
    private final int bitSize;
    private final int numHashFunctions;
    private final BitSet bitSet;

    public BloomFilter(int expectedEntries, double falsePositiveRate) {
        if (expectedEntries <= 0) {
            expectedEntries = 1;
        }
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            throw new IllegalArgumentException("False positive rate must be between 0 and 1");
        }

        // Calculate optimal bit size: m = -(n * ln(p)) / (ln(2)^2)
        double ln2 = Math.log(2);
        long m = (long) Math.ceil(-1.0 * expectedEntries * Math.log(falsePositiveRate) / (ln2 * ln2));
        if (m <= 0 || m > Integer.MAX_VALUE - 64) {
            m = 64;
        }
        this.bitSize = (int) Math.max(64, m);

        // Calculate optimal hash functions: k = (m / n) * ln(2)
        int k = (int) Math.round(((double) this.bitSize / expectedEntries) * ln2);
        this.numHashFunctions = Math.max(1, Math.min(30, k));
        this.bitSet = new BitSet(this.bitSize);
    }

    private BloomFilter(int bitSize, int numHashFunctions, BitSet bitSet) {
        this.bitSize = bitSize;
        this.numHashFunctions = numHashFunctions;
        this.bitSet = bitSet;
    }

    /**
     * Adds a key to the Bloom filter.
     */
    public void add(ByteKey key) {
        byte[] bytes = key.getBytes();
        long hash1 = hash64(bytes, 0);
        long hash2 = hash64(bytes, hash1);

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + i * hash2;
            int bitIndex = (int) Math.floorMod(combinedHash, (long) bitSize);
            bitSet.set(bitIndex);
        }
    }

    /**
     * Checks if the key might be present in the Bloom filter.
     * If false, the key is 100% guaranteed not to be present.
     */
    public boolean mightContain(ByteKey key) {
        byte[] bytes = key.getBytes();
        long hash1 = hash64(bytes, 0);
        long hash2 = hash64(bytes, hash1);

        for (int i = 0; i < numHashFunctions; i++) {
            long combinedHash = hash1 + i * hash2;
            int bitIndex = (int) Math.floorMod(combinedHash, (long) bitSize);
            if (!bitSet.get(bitIndex)) {
                return false;
            }
        }
        return true;
    }

    public int getBitSize() {
        return bitSize;
    }

    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    /**
     * Serializes this Bloom Filter into a ByteBuffer.
     * Format:
     * [BitSize: 4B][NumHashFunctions: 4B][LongArrayLength: 4B][LongArray: L * 8B]
     */
    public ByteBuffer serialize() {
        long[] words = bitSet.toLongArray();
        int bufferSize = 4 + 4 + 4 + words.length * 8;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        buffer.putInt(bitSize);
        buffer.putInt(numHashFunctions);
        buffer.putInt(words.length);
        for (long word : words) {
            buffer.putLong(word);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Deserializes a Bloom Filter from a ByteBuffer.
     */
    public static BloomFilter deserialize(ByteBuffer buffer) {
        int bitSize = buffer.getInt();
        int numHashFunctions = buffer.getInt();
        int wordsLength = buffer.getInt();
        long[] words = new long[wordsLength];
        for (int i = 0; i < wordsLength; i++) {
            words[i] = buffer.getLong();
        }
        BitSet bitSet = BitSet.valueOf(words);
        return new BloomFilter(bitSize, numHashFunctions, bitSet);
    }

    /**
     * Fast 64-bit MurmurHash3 variant.
     */
    private static long hash64(byte[] data, long seed) {
        long h = seed ^ (data.length * 0xc6a4a7935bd1e995L);
        int length = data.length;
        int numBlocks = length / 8;

        for (int i = 0; i < numBlocks; i++) {
            int offset = i * 8;
            long k = ((long) data[offset] & 0xff)
                    | (((long) data[offset + 1] & 0xff) << 8)
                    | (((long) data[offset + 2] & 0xff) << 16)
                    | (((long) data[offset + 3] & 0xff) << 24)
                    | (((long) data[offset + 4] & 0xff) << 32)
                    | (((long) data[offset + 5] & 0xff) << 40)
                    | (((long) data[offset + 6] & 0xff) << 48)
                    | (((long) data[offset + 7] & 0xff) << 56);

            k *= 0xc6a4a7935bd1e995L;
            k ^= k >>> 47;
            k *= 0xc6a4a7935bd1e995L;

            h ^= k;
            h *= 0xc6a4a7935bd1e995L;
        }

        int tailOffset = numBlocks * 8;
        int remaining = length - tailOffset;
        if (remaining > 0) {
            long k = 0;
            for (int i = remaining - 1; i >= 0; i--) {
                k = (k << 8) | ((long) data[tailOffset + i] & 0xff);
            }
            k *= 0xc6a4a7935bd1e995L;
            k ^= k >>> 47;
            k *= 0xc6a4a7935bd1e995L;
            h ^= k;
        }

        h ^= h >>> 47;
        h *= 0xc6a4a7935bd1e995L;
        h ^= h >>> 47;

        return h;
    }
}
