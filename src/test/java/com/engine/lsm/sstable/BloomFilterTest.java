package com.engine.lsm.sstable;

import com.engine.lsm.model.ByteKey;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {

    @Test
    @DisplayName("Bloom filter guarantees 100% True Positive rate (Zero False Negatives)")
    void testZeroFalseNegatives() {
        int n = 5000;
        BloomFilter filter = new BloomFilter(n, 0.01);

        for (int i = 0; i < n; i++) {
            filter.add(ByteKey.of("member-" + i));
        }

        // Every inserted key MUST return true
        for (int i = 0; i < n; i++) {
            assertTrue(filter.mightContain(ByteKey.of("member-" + i)),
                    "Key 'member-" + i + "' was inserted but Bloom filter returned false (False Negative violation)");
        }
    }

    @Test
    @DisplayName("False positive rate is bounded by theoretical target (within margin of error)")
    void testFalsePositiveBound() {
        int n = 10000;
        double targetFp = 0.01; // 1%
        BloomFilter filter = new BloomFilter(n, targetFp);

        for (int i = 0; i < n; i++) {
            filter.add(ByteKey.of("present-key-" + i));
        }

        int falsePositives = 0;
        int testCount = 10000;
        for (int i = 0; i < testCount; i++) {
            if (filter.mightContain(ByteKey.of("absent-key-" + i))) {
                falsePositives++;
            }
        }

        double empiricalFpRate = (double) falsePositives / testCount;
        // With 10,000 samples, empirical rate for 1% target should easily be below 2.5%
        assertTrue(empiricalFpRate < 0.025,
                "Empirical FP rate " + empiricalFpRate + " exceeded acceptable bound for target " + targetFp);
    }

    @Test
    @DisplayName("Serialization and deserialization preserves exact bit set and parameters")
    void testSerializationFidelity() {
        BloomFilter original = new BloomFilter(1000, 0.01);
        for (int i = 0; i < 500; i++) {
            original.add(ByteKey.of("item-" + i));
        }

        ByteBuffer buffer = original.serialize();
        BloomFilter restored = BloomFilter.deserialize(buffer);

        assertEquals(original.getBitSize(), restored.getBitSize());
        assertEquals(original.getNumHashFunctions(), restored.getNumHashFunctions());

        for (int i = 0; i < 500; i++) {
            assertTrue(restored.mightContain(ByteKey.of("item-" + i)));
        }
        assertFalse(restored.mightContain(ByteKey.of("never-added-xyz")));
    }
}
