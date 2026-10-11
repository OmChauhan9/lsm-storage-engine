package com.engine.lsm.wal;

import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.model.RecordType;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Append-only Write-Ahead Log (WAL) backed by NIO {@link FileChannel}.
 * Guarantees persistence of incoming mutations before memory modifications.
 */
public class WriteAheadLog implements Closeable {
    private final Path logPath;
    private final FileChannel channel;
    private final boolean syncOnWrite;
    private final AtomicLong bytesWritten = new AtomicLong(0);

    public WriteAheadLog(Path logPath, boolean syncOnWrite) throws IOException {
        this.logPath = logPath;
        this.syncOnWrite = syncOnWrite;

        // Ensure parent directories exist
        Path parent = logPath.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }

        this.channel = FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
        this.bytesWritten.set(channel.size());
    }

    public synchronized void appendPut(ByteKey key, byte[] value) throws IOException {
        append(RecordType.PUT, key.getBytes(), value != null ? value : Record.EMPTY_BYTES);
    }

    public synchronized void appendDelete(ByteKey key) throws IOException {
        append(RecordType.DELETE, key.getBytes(), Record.EMPTY_BYTES);
    }

    public synchronized void appendRecord(Record record) throws IOException {
        ByteBuffer buffer = WalRecord.serialize(record);
        int frameLength = buffer.remaining();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        if (syncOnWrite) {
            channel.force(false);
        }
        bytesWritten.addAndGet(frameLength);
    }

    private void append(RecordType type, byte[] keyBytes, byte[] valBytes) throws IOException {
        ByteBuffer buffer = WalRecord.serialize(type, keyBytes, valBytes);
        int frameLength = buffer.remaining();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        if (syncOnWrite) {
            channel.force(false);
        }
        bytesWritten.addAndGet(frameLength);
    }

    public Path getLogPath() {
        return logPath;
    }

    public long getBytesWritten() {
        return bytesWritten.get();
    }

    public synchronized void sync() throws IOException {
        if (channel.isOpen()) {
            channel.force(false);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (channel.isOpen()) {
            channel.force(true);
            channel.close();
        }
    }

    /**
     * Closes and permanently deletes this log file from disk.
     * Invoked after the corresponding MemTable has been safely flushed to an SSTable.
     */
    public synchronized void delete() throws IOException {
        close();
        Files.deleteIfExists(logPath);
    }
}
