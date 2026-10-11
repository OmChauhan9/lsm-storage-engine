package com.engine.lsm.model;

/**
 * Enumeration representing the operational record type stored in WAL, MemTable, and SSTables.
 */
public enum RecordType {
    PUT((byte) 0x01),
    DELETE((byte) 0x02);

    private final byte code;

    RecordType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static RecordType fromCode(byte code) {
        return switch (code) {
            case 0x01 -> PUT;
            case 0x02 -> DELETE;
            default -> throw new IllegalArgumentException("Unknown RecordType code: 0x" + Integer.toHexString(code & 0xFF));
        };
    }
}
