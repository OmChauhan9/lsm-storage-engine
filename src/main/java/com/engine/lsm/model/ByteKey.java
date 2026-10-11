package com.engine.lsm.model;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Immutable wrapper around a byte array that implements lexicographical ordering.
 * <p>
 * Standard Java {@code byte[]} does not implement {@link Comparable} and its {@code equals()}
 * and {@code hashCode()} rely on identity reference. {@code ByteKey} provides content-based
 * hashing and uses vector-optimized unsigned byte comparisons via {@link Arrays#compareUnsigned}.
 */
public final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;
    private final int hashCode;

    public ByteKey(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Key bytes cannot be null");
        }
        // Defensive copy to ensure immutability
        this.bytes = bytes.clone();
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    public static ByteKey of(byte[] bytes) {
        return new ByteKey(bytes);
    }

    public static ByteKey of(String utf8String) {
        if (utf8String == null) {
            throw new IllegalArgumentException("Key string cannot be null");
        }
        return new ByteKey(utf8String.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public int length() {
        return bytes.length;
    }

    public String asUtf8String() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(ByteKey other) {
        if (other == null) {
            throw new NullPointerException("Cannot compare with null ByteKey");
        }
        return Arrays.compareUnsigned(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteKey byteKey = (ByteKey) o;
        return Arrays.equals(bytes, byteKey.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        // Render readable string if all characters are printable ASCII, otherwise hex
        boolean printable = true;
        for (byte b : bytes) {
            if (b < 32 || b > 126) {
                printable = false;
                break;
            }
        }
        if (printable && bytes.length > 0) {
            return "ByteKey[\"" + asUtf8String() + "\"]";
        }
        StringBuilder sb = new StringBuilder("ByteKey[0x");
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        sb.append("]");
        return sb.toString();
    }
}
