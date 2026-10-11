package com.engine.lsm;

import com.engine.lsm.compaction.CompactionResult;
import com.engine.lsm.compaction.Compactor;
import com.engine.lsm.memtable.MemTable;
import com.engine.lsm.model.ByteKey;
import com.engine.lsm.model.Record;
import com.engine.lsm.sstable.SSTableReader;
import com.engine.lsm.sstable.SSTableWriter;
import com.engine.lsm.wal.WalRecovery;
import com.engine.lsm.wal.WriteAheadLog;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Single-Node Log-Structured Merge (LSM) Storage Engine.
 * <p>
 * Architecture:
 * <ul>
 *   <li><b>Writes:</b> Inbound writes append sequentially to an on-disk binary Write-Ahead Log (WAL)
 *       and insert into an in-memory thread-safe ConcurrentSkipListMap MemTable with atomic byte accounting.</li>
 *   <li><b>Flushes:</b> When the MemTable hits configured byte threshold, it freezes as an Immutable MemTable
 *       and flushes to an immutable on-disk Sorted String Table (SSTable) containing sorted key-value blocks,
 *       a sparse index block, a Bloom filter bitset, and a fixed 32-byte footer.</li>
 *   <li><b>Reads:</b> Tiered lookups check Active MemTable &rarr; Immutable MemTable &rarr; On-Disk SSTables
 *       (ordered newest to oldest) utilizing Bloom filters to suppress disk I/O and Sparse Index binary search
 *       to locate bounded disk block ranges.</li>
 *   <li><b>Compaction:</b> Streaming multi-way merge sort merges adjacent SSTables, deduplicates overwritten
 *       keys, and purges obsolete tombstones to control space and read amplification.</li>
 * </ul>
 */
public class LsmEngine implements Closeable {
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("sstable-(\\d+)\\.db");
    private static final Pattern WAL_PATTERN = Pattern.compile("wal-(\\d+)\\.log");

    private final Config config;
    private final AtomicLong sequenceNumberGenerator = new AtomicLong(0);
    private final AtomicLong tableIdGenerator = new AtomicLong(0);
    private final AtomicLong walSegmentGenerator = new AtomicLong(0);

    private final ReentrantLock writeLock = new ReentrantLock();
    private volatile WriteAheadLog currentWal;
    private volatile MemTable activeMemTable;
    private final AtomicReference<MemTable> immutableMemTable = new AtomicReference<>(null);

    // CopyOnWriteArrayList gives lock-free, thread-safe snapshot iteration for readers.
    // Ordered strictly from NEWEST to OLDEST SSTable.
    private final CopyOnWriteArrayList<SSTableReader> sstables = new CopyOnWriteArrayList<>();

    private final ExecutorService flushExecutor;
    private final ExecutorService compactionExecutor;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private LsmEngine(Config config) throws IOException {
        this.config = config;
        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush-worker");
            t.setDaemon(true);
            return t;
        });
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-compaction-worker");
            t.setDaemon(true);
            return t;
        });

        // Initialize directories
        Files.createDirectories(config.getWalDir());
        Files.createDirectories(config.getSstableDir());

        // Discover and load existing SSTables
        recoverSSTables();

        // Replay WAL files and initialize active state
        recoverWalAndInitialize();
    }

    public static LsmEngine open(Config config) throws IOException {
        return new LsmEngine(config);
    }

    public static LsmEngine open(Path dataDir) throws IOException {
        return new LsmEngine(Config.defaultConfig(dataDir));
    }

    // =========================================================================
    // Public CRUD API
    // =========================================================================

    public void put(byte[] key, byte[] value) throws IOException {
        put(ByteKey.of(key), value);
    }

    public void put(String key, String value) throws IOException {
        put(
                ByteKey.of(key),
                (value != null) ? value.getBytes(StandardCharsets.UTF_8) : Record.EMPTY_BYTES
        );
    }

    public void put(ByteKey key, byte[] value) throws IOException {
        ensureOpen();
        writeLock.lock();
        try {
            checkAndTriggerFlush();

            long seq = sequenceNumberGenerator.incrementAndGet();
            currentWal.appendPut(key, value);
            activeMemTable.put(key, value, seq);
        } finally {
            writeLock.unlock();
        }
    }

    public byte[] get(byte[] key) throws IOException {
        return get(ByteKey.of(key));
    }

    public String get(String key) throws IOException {
        byte[] bytes = get(ByteKey.of(key));
        return (bytes != null) ? new String(bytes, StandardCharsets.UTF_8) : null;
    }

    /**
     * Tiered Read Path:
     * 1. Active MemTable
     * 2. Immutable MemTable (if flush in-flight)
     * 3. On-disk SSTables (newest to oldest)
     */
    public byte[] get(ByteKey key) throws IOException {
        ensureOpen();

        // 1. Check Active MemTable
        Record activeRecord = activeMemTable.get(key);
        if (activeRecord != null) {
            return activeRecord.isTombstone() ? null : activeRecord.getValue();
        }

        // 2. Check Immutable MemTable
        MemTable imm = immutableMemTable.get();
        if (imm != null) {
            Record immRecord = imm.get(key);
            if (immRecord != null) {
                return immRecord.isTombstone() ? null : immRecord.getValue();
            }
        }

        // 3. Check On-Disk SSTables (newest to oldest)
        for (SSTableReader reader : sstables) {
            Record diskRecord = reader.get(key);
            if (diskRecord != null) {
                return diskRecord.isTombstone() ? null : diskRecord.getValue();
            }
        }

        return null;
    }

    public void delete(byte[] key) throws IOException {
        delete(ByteKey.of(key));
    }

    public void delete(String key) throws IOException {
        delete(ByteKey.of(key));
    }

    public void delete(ByteKey key) throws IOException {
        ensureOpen();
        writeLock.lock();
        try {
            checkAndTriggerFlush();

            long seq = sequenceNumberGenerator.incrementAndGet();
            currentWal.appendDelete(key);
            activeMemTable.delete(key, seq);
        } finally {
            writeLock.unlock();
        }
    }

    // =========================================================================
    // Flush Lifecycle
    // =========================================================================

    private void checkAndTriggerFlush() throws IOException {
        if (activeMemTable.getByteSize() >= config.getMemTableThresholdBytes()) {
            triggerFlushInternal();
        }
    }

    /**
     * Public manual flush trigger. Waits for pending flush to complete.
     */
    public void flush() throws IOException {
        ensureOpen();
        writeLock.lock();
        try {
            if (!activeMemTable.isEmpty()) {
                triggerFlushInternal();
            }
        } finally {
            writeLock.unlock();
        }
        awaitPendingFlush();
    }

    private void triggerFlushInternal() throws IOException {
        // Wait if a previous flush is still completing
        awaitPendingFlush();

        // Freeze current active MemTable
        activeMemTable.freeze();
        immutableMemTable.set(activeMemTable);

        // Close current WAL and rotate
        long oldWalSegmentId = activeMemTable.getWalSegmentId();
        currentWal.close();

        long newWalSegmentId = walSegmentGenerator.incrementAndGet();
        Path newWalPath = getWalPath(newWalSegmentId);
        this.currentWal = new WriteAheadLog(newWalPath, config.isSyncWalOnWrite());
        this.activeMemTable = new MemTable(newWalSegmentId);

        // Submit flush task to background worker
        MemTable tableToFlush = immutableMemTable.get();
        flushExecutor.submit(() -> {
            try {
                executeFlush(tableToFlush, oldWalSegmentId);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        });
    }

    private void executeFlush(MemTable tableToFlush, long oldWalSegmentId) throws IOException {
        if (tableToFlush.isEmpty()) {
            immutableMemTable.set(null);
            return;
        }

        long tableId = tableIdGenerator.incrementAndGet();
        Path sstablePath = getSstablePath(tableId);

        // Write contiguous blocks, sparse index, bloom filter, and footer
        SSTableWriter.writeFromMemTable(
                sstablePath,
                tableToFlush,
                config.getIndexStride(),
                config.getBloomFilterFpRate()
        );

        // Open reader and register as newest SSTable (index 0)
        SSTableReader reader = SSTableReader.open(sstablePath, tableId);
        sstables.add(0, reader);

        // Safely delete the old WAL log file now that data is persisted in SSTable
        Path oldWalPath = getWalPath(oldWalSegmentId);
        Files.deleteIfExists(oldWalPath);

        // Clear immutable reference
        immutableMemTable.set(null);

        // Check if compaction should be triggered
        checkAndTriggerCompaction();
    }

    private void awaitPendingFlush() {
        while (immutableMemTable.get() != null) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while awaiting flush", e);
            }
        }
    }

    // =========================================================================
    // Compaction Lifecycle
    // =========================================================================

    private void checkAndTriggerCompaction() {
        if (!isClosed.get() && sstables.size() >= config.getCompactionThreshold()) {
            compactionExecutor.submit(() -> {
                try {
                    if (!isClosed.get()) {
                        compact();
                    }
                } catch (IllegalStateException ignored) {
                    // Engine closed concurrently, ignore safely
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            });
        }
    }

    /**
     * Public manual compaction trigger. Executes multi-way streaming merge-sort
     * across all active SSTables, eliminating duplicates and reclaimed tombstones.
     */
    public synchronized CompactionResult compact() throws IOException {
        ensureOpen();
        List<SSTableReader> tablesToCompact = new ArrayList<>(sstables);
        if (tablesToCompact.size() < 2) {
            return new CompactionResult(null, Collections.emptyList(), 0, 0, 0);
        }

        long newTableId = tableIdGenerator.incrementAndGet();
        Path compactedPath = getSstablePath(newTableId);

        // If compacting all active SSTables, this is the bottom tier (safe to drop tombstones)
        boolean isBottomTier = (tablesToCompact.size() == sstables.size());

        CompactionResult result = Compactor.compact(
                tablesToCompact,
                compactedPath,
                newTableId,
                isBottomTier,
                config
        );

        // Atomically update sstables list
        writeLock.lock();
        try {
            sstables.removeAll(tablesToCompact);
            sstables.add(0, result.compactedReader());
            // Sort to ensure strict newest-first ordering
            sstables.sort(Comparator.naturalOrder());
        } finally {
            writeLock.unlock();
        }

        // Clean up obsolete SSTables from disk
        for (SSTableReader oldReader : tablesToCompact) {
            try {
                oldReader.delete();
            } catch (IOException ignored) {}
        }

        return result;
    }

    // =========================================================================
    // Recovery & Startup
    // =========================================================================

    private void recoverSSTables() throws IOException {
        List<Path> sstableFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(config.getSstableDir(), "sstable-*.db")) {
            for (Path p : stream) {
                sstableFiles.add(p);
            }
        }

        long maxTableId = 0;
        List<SSTableReader> loaded = new ArrayList<>();

        for (Path p : sstableFiles) {
            Matcher matcher = SSTABLE_PATTERN.matcher(p.getFileName().toString());
            if (matcher.matches()) {
                long tid = Long.parseLong(matcher.group(1));
                maxTableId = Math.max(maxTableId, tid);
                try {
                    SSTableReader reader = SSTableReader.open(p, tid);
                    loaded.add(reader);
                } catch (Exception e) {
                    System.err.println("Warning: failed to load SSTable " + p + ": " + e.getMessage());
                }
            }
        }

        tableIdGenerator.set(maxTableId);
        // Sort newest first (descending by tableId)
        loaded.sort(Comparator.naturalOrder());
        sstables.addAll(loaded);
    }

    private void recoverWalAndInitialize() throws IOException {
        List<Path> walFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(config.getWalDir(), "wal-*.log")) {
            for (Path p : stream) {
                walFiles.add(p);
            }
        }

        // Sort WAL files chronologically
        walFiles.sort(Comparator.comparing(Path::getFileName));

        long maxWalId = 0;
        MemTable recoveredTable = new MemTable(0);

        for (Path walPath : walFiles) {
            Matcher matcher = WAL_PATTERN.matcher(walPath.getFileName().toString());
            if (matcher.matches()) {
                long wid = Long.parseLong(matcher.group(1));
                maxWalId = Math.max(maxWalId, wid);

                WalRecovery.RecoveryResult result = WalRecovery.recover(walPath, sequenceNumberGenerator);
                for (Record rec : result.records()) {
                    if (rec.isTombstone()) {
                        recoveredTable.delete(rec.getKey(), rec.getSequenceNumber());
                    } else {
                        recoveredTable.put(rec.getKey(), rec.getValue(), rec.getSequenceNumber());
                    }
                }
            }
        }

        walSegmentGenerator.set(maxWalId);

        // If recovered records exist, flush them to an SSTable or adopt them
        long nextWalId = walSegmentGenerator.incrementAndGet();
        Path activeWalPath = getWalPath(nextWalId);

        if (!recoveredTable.isEmpty()) {
            if (recoveredTable.getByteSize() >= config.getMemTableThresholdBytes()) {
                // Immediately flush recovered data to SSTable
                long tid = tableIdGenerator.incrementAndGet();
                Path sstPath = getSstablePath(tid);
                SSTableWriter.writeFromMemTable(sstPath, recoveredTable, config.getIndexStride(), config.getBloomFilterFpRate());
                SSTableReader r = SSTableReader.open(sstPath, tid);
                sstables.add(0, r);

                // Clean up old replayed WAL files
                for (Path p : walFiles) {
                    Files.deleteIfExists(p);
                }

                this.activeMemTable = new MemTable(nextWalId);
                this.currentWal = new WriteAheadLog(activeWalPath, config.isSyncWalOnWrite());
            } else {
                // Adopt recovered records into active MemTable
                this.activeMemTable = recoveredTable;
                this.currentWal = new WriteAheadLog(activeWalPath, config.isSyncWalOnWrite());
                // Rewrite recovered records to active WAL for ongoing durability
                for (Map.Entry<ByteKey, Record> entry : recoveredTable) {
                    this.currentWal.appendRecord(entry.getValue());
                }
                // Clean up old WAL segments
                for (Path p : walFiles) {
                    if (!p.equals(activeWalPath)) {
                        Files.deleteIfExists(p);
                    }
                }
            }
        } else {
            // Clean slate
            this.activeMemTable = new MemTable(nextWalId);
            this.currentWal = new WriteAheadLog(activeWalPath, config.isSyncWalOnWrite());
        }
    }

    // =========================================================================
    // Inspection and Diagnostic Helpers
    // =========================================================================

    public List<SSTableReader> getSstables() {
        return Collections.unmodifiableList(sstables);
    }

    public int getActiveMemTableEntryCount() {
        return activeMemTable.entryCount();
    }

    public long getActiveMemTableByteSize() {
        return activeMemTable.getByteSize();
    }

    public Config getConfig() {
        return config;
    }

    private Path getWalPath(long segmentId) {
        return config.getWalDir().resolve(String.format("wal-%010d.log", segmentId));
    }

    private Path getSstablePath(long tableId) {
        return config.getSstableDir().resolve(String.format("sstable-%010d.db", tableId));
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LsmEngine has been closed");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            // Await any in-flight flush
            awaitPendingFlush();

            // Flush remaining data in active MemTable if non-empty
            if (activeMemTable != null && !activeMemTable.isEmpty()) {
                long tableId = tableIdGenerator.incrementAndGet();
                Path sstablePath = getSstablePath(tableId);
                SSTableWriter.writeFromMemTable(
                        sstablePath,
                        activeMemTable,
                        config.getIndexStride(),
                        config.getBloomFilterFpRate()
                );
                SSTableReader reader = SSTableReader.open(sstablePath, tableId);
                sstables.add(0, reader);

                // Delete backing WAL
                if (currentWal != null) {
                    currentWal.delete();
                }
            } else if (currentWal != null) {
                currentWal.close();
            }

            // Shutdown executors
            flushExecutor.shutdown();
            compactionExecutor.shutdown();
            try {
                flushExecutor.awaitTermination(5, TimeUnit.SECONDS);
                compactionExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {}

            // Close all SSTable readers
            for (SSTableReader reader : sstables) {
                reader.close();
            }
        }
    }
}
