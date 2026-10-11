package com.engine.lsm.wal;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Handles encoding and decoding of binary frames for WAL and SSTable data blocks.
 * <p>
 * Frame Structure (13 + K + V bytes):
 * <ul>
 *   <li>CRC32 Checksum: 4 Bytes (unsigned 32-bit int computed over payload)</li>
 *   <li>Record Type: 1 Byte (0x01 = PUT, 0x02 = DELETE)</li>
 *   <li>Key Length (K): 4 Bytes (signed 32-bit int)</li>
 *   <li>Value Length (V): 4 Bytes (signed 32-bit int)</li>
 *   <li>Key Bytes: K Bytes</li>
 *   <li>Value Bytes: V Bytes</li>
 * </ul>
 */
public final class WalRecord {
    public static final int HEADER_METADATA_SIZE = 13; // 4 (CRC) + 1 (Type) + 4 (KeyLen) + 4 (ValLen)

    private WalRecord() {}

    /**
     * Serializes a Record into a freshly allocated ByteBuffer with computed CRC32.
     */
    public static ByteBuffer serialize(Record record) {
        byte[] keyBytes = record.getKey().getBytes();
        byte[] valBytes = record.getValue();
        int payloadSize = 1 + 4 + 4 + keyBytes.length + valBytes.length;
        int totalFrameSize = 4 + payloadSize;

        ByteBuffer buffer = ByteBuffer.allocate(totalFrameSize);
        // Skip 4 bytes for CRC
        buffer.position(4);
        buffer.put(record.getType().getCode());
        buffer.putInt(keyBytes.length);
        buffer.putInt(valBytes.length);
        buffer.put(keyBytes);
        buffer.put(valBytes);

        // Compute CRC32 over the payload
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 4, payloadSize);
        int checksum = (int) crc.getValue();

        // Write CRC32 at offset 0
        buffer.putInt(0, checksum);
        buffer.position(0);
        return buffer;
    }

    /**
     * Serializes a PUT or DELETE mutation without creating an intermediate Record object.
     */
    public static ByteBuffer serialize(RecordType type, byte[] keyBytes, byte[] valBytes) {
        byte[] safeValBytes = (valBytes != null) ? valBytes : Record.EMPTY_BYTES;
        int payloadSize = 1 + 4 + 4 + keyBytes.length + safeValBytes.length;
        int totalFrameSize = 4 + payloadSize;

        ByteBuffer buffer = ByteBuffer.allocate(totalFrameSize);
        buffer.position(4);
        buffer.put(type.getCode());
        buffer.putInt(keyBytes.length);
        buffer.putInt(safeValBytes.length);
        buffer.put(keyBytes);
        buffer.put(safeValBytes);

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 4, payloadSize);
        int checksum = (int) crc.getValue();

        buffer.putInt(0, checksum);
        buffer.position(0);
        return buffer;
    }
}
