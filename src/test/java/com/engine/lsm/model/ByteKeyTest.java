package com.engine.lsm.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ByteKeyTest {

    @Test
    @DisplayName("Lexicographical unsigned comparison orders bytes correctly")
    void testUnsignedOrdering() {
        ByteKey k1 = ByteKey.of(new byte[]{0x01});
        ByteKey k2 = ByteKey.of(new byte[]{0x02});
        ByteKey k3 = ByteKey.of(new byte[]{(byte) 0x80}); // 128 in unsigned, -128 in signed byte
        ByteKey k4 = ByteKey.of(new byte[]{(byte) 0xFF}); // 255 in unsigned, -1 in signed byte

        assertTrue(k1.compareTo(k2) < 0);
        assertTrue(k2.compareTo(k3) < 0, "0x02 must be less than 0x80 in unsigned order");
        assertTrue(k3.compareTo(k4) < 0, "0x80 must be less than 0xFF in unsigned order");
    }

    @Test
    @DisplayName("Prefix ordering: shorter prefix is smaller")
    void testPrefixOrdering() {
        ByteKey prefix = ByteKey.of("apple");
        ByteKey longer = ByteKey.of("apples");

        assertTrue(prefix.compareTo(longer) < 0);
        assertTrue(longer.compareTo(prefix) > 0);
    }

    @Test
    @DisplayName("Equality and HashCode rely on byte content, not reference")
    void testEqualityAndHash() {
        byte[] arr1 = new byte[]{10, 20, 30};
        byte[] arr2 = new byte[]{10, 20, 30};

        ByteKey key1 = ByteKey.of(arr1);
        ByteKey key2 = ByteKey.of(arr2);

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
        assertEquals(0, key1.compareTo(key2));
    }

    @Test
    @DisplayName("ByteKey defends against external array mutation")
    void testDefensiveCopy() {
        byte[] raw = new byte[]{1, 2, 3};
        ByteKey key = ByteKey.of(raw);

        // Mutate original array
        raw[0] = 99;

        // ByteKey internal bytes must remain unmutated
        assertArrayEquals(new byte[]{1, 2, 3}, key.getBytes());
    }
}
