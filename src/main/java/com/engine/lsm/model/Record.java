package com.engine.lsm.model;

import java.util.Arrays;

/**
 * In-memory representation of an individual key-value mutation in the LSM engine.
 */
public final class Record {
    public static final byte[] EMPTY_BYTES = new byte[0];

    // Estimated 64-bit JVM overhead per entry:
    // Object headers, references, SkipList Node overhead, and array headers.
    public static final int ESTIMATED_OVERHEAD_BYTES = 128;

    private final ByteKey key;
    private final byte[] value;
    private final RecordType type;
    private final long sequenceNumber;

    public Record(ByteKey key, byte[] value, RecordType type, long sequenceNumber) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("RecordType cannot be null");
        }
        this.key = key;
        this.value = (value != null) ? value.clone() : EMPTY_BYTES;
        this.type = type;
        this.sequenceNumber = sequenceNumber;
    }

    public static Record put(ByteKey key, byte[] value, long sequenceNumber) {
        return new Record(key, value, RecordType.PUT, sequenceNumber);
    }

    public static Record delete(ByteKey key, long sequenceNumber) {
        return new Record(key, EMPTY_BYTES, RecordType.DELETE, sequenceNumber);
    }

    public ByteKey getKey() {
        return key;
    }

    public byte[] getValue() {
        return value.clone();
    }

    public RecordType getType() {
        return type;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public boolean isTombstone() {
        return type == RecordType.DELETE;
    }

    /**
     * Calculates the exact serialized on-disk frame size in bytes:
     * [CRC32: 4B] + [Type: 1B] + [KeyLen: 4B] + [ValLen: 4B] + [Key: K] + [Val: V] = 13 + K + V
     */
    public int serializedSize() {
        return 13 + key.length() + value.length;
    }

    /**
     * Conservative estimation of memory occupied on the JVM heap including object headers,
     * array padding, and ConcurrentSkipListMap node pointers.
     */
    public long estimatedHeapSize() {
        return ESTIMATED_OVERHEAD_BYTES + key.length() + value.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return sequenceNumber == record.sequenceNumber &&
               key.equals(record.key) &&
               type == record.type &&
               Arrays.equals(value, record.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + type.hashCode();
        result = 31 * result + Long.hashCode(sequenceNumber);
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "key=" + key +
                ", type=" + type +
                ", valLen=" + value.length +
                ", seq=" + sequenceNumber +
                '}';
    }
}
