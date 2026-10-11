# LSM Storage Engine: End-to-End Workflow Execution Guide

A granular, step-by-step systems manual detailing the exact operational paths, byte-level packet flows, and state transitions of the Single-Node LSM Storage Engine.

---

## 1. Master System Architecture Map

```mermaid
flowchart TD
    subgraph Client ["Client Interface"]
        C_Put["put(key, value) / delete(key)"]
        C_Get["get(key)"]
    end

    subgraph WritePath ["Write Path (Sequential Append)"]
        WL["Acquire writeLock"]
        WAL["Write-Ahead Log (WAL)\n[CRC32][Type][KLen][VLen][Key][Val]\nFileChannel.write / optional fsync"]
        MT["Active MemTable\nConcurrentSkipListMap<ByteKey, Record>\nAtomic Memory Accounting"]
    end

    subgraph FlushPath ["Flush Pipeline (RAM to Disk)"]
        Freeze["activeMemTable.freeze()\nimmutableMemTable.set()"]
        Rotate["Rotate WAL -> wal-0002.log\nAllocate fresh activeMemTable"]
        Worker["flushExecutor Background Task"]
        SST_Write["SSTableWriter\n1. Contiguous Data Block\n2. Sparse Index Block\n3. Bloom Filter Block\n4. Fixed 32-Byte Footer"]
        Register["sstables.add(0, newReader)\nDelete old wal-0001.log"]
    end

    subgraph ReadPath ["Tiered Read Path (Lock-Free)"]
        CheckActive{"Check Active MemTable"}
        CheckImm{"Check Immutable MemTable"}
        CheckDisk{"Iterate SSTables\n(Newest to Oldest)"}
        Bloom{"BloomFilter.mightContain()"}
        Sparse["SparseIndex Binary Search\nFind [startOffset, endOffset]"]
        DiskSeek["Positional FileChannel.read()\nScan small contiguous block"]
        Found["Return Value / Null if Tombstone"]
        NotFound["Return Null (Key Not Found)"]
    end

    subgraph CompactionPath ["Compaction Engine (Streaming Merge-Sort)"]
        CompTrigger{"SSTables >= Threshold?"}
        MinHeap["k-Way Merge-Sort PriorityQueue\nDrain duplicates; keep newest record"]
        TombstoneGC{"Is Bottom Tier?"}
        DropTomb["Purge Tombstone"]
        KeepTomb["Preserve Tombstone"]
        NewSST["Write Compacted SSTable"]
        Swap["Atomically Swap Reader List\nDelete Old SSTable Files"]
    end

    C_Put --> WL --> WAL --> MT
    MT -->|Memory Threshold Exceeded| Freeze --> Rotate --> Worker --> SST_Write --> Register
    Register --> CompTrigger

    C_Get --> CheckActive
    CheckActive -->|Match| Found
    CheckActive -->|Miss| CheckImm
    CheckImm -->|Match| Found
    CheckImm -->|Miss| CheckDisk
    CheckDisk -->|Next SSTable| Bloom
    Bloom -->|False (Suppressed)| CheckDisk
    Bloom -->|True| Sparse --> DiskSeek
    DiskSeek -->|Match| Found
    DiskSeek -->|Miss| CheckDisk
    CheckDisk -->|All Exhausted| NotFound

    CompTrigger -->|Yes| MinHeap
    MinHeap --> TombstoneGC
    TombstoneGC -->|Yes| DropTomb --> NewSST
    TombstoneGC -->|No| KeepTomb --> NewSST
    NewSST --> Swap
```

---

## 2. Workflow 1: The Inbound Write Lifecycle (`put` & `delete`)

Every write operation executes sequentially through memory and disk:

```
[Client] ──> engine.put(key, val)
                │
                ▼
         [ReentrantLock] (Ensures sequential WAL appends and clean rotations)
                │
                ▼
         [Threshold Check] ──> If MemTable full: freeze & trigger flush
                │
                ▼
         [WAL Serializer] ──> Builds 13 + K + V byte frame with CRC32
                │
                ▼
         [FileChannel] ────> Sequential append to on-disk WAL
                │
                ▼
         [MemTable] ───────> Lock-free CAS insert into ConcurrentSkipListMap
                │
                ▼
         [MemoryTracker] ──> Atomic counter increments heap size
                │
                ▼
         [Release Lock] ───> Return success to client
```

### Granular Execution Steps
1. **API Invocation:** Client calls `engine.put(key, value)` or `engine.delete(key)`. If `delete`, value is represented as `byte[0]` with `RecordType.DELETE`.
2. **Lock Acquisition:** The thread acquires `writeLock`. (Note: Readers do **not** acquire this lock, maintaining lock-free read concurrency).
3. **Capacity Evaluation:** `activeMemTable.getByteSize()` is checked against `config.getMemTableThresholdBytes()`. If the threshold is breached, the MemTable rotation sub-workflow triggers before continuing.
4. **Sequence Numbering:** Monotonically increasing sequence number is generated via `sequenceNumberGenerator.incrementAndGet()`.
5. **Binary Frame Encoding:**
   * A contiguous `ByteBuffer` of size $13 + K + V$ is allocated.
   * Header fields are packed: `[Type: 1B]`, `[KeyLen: 4B]`, `[ValLen: 4B]`, `[Key: K bytes]`, `[Value: V bytes]`.
   * Standard IEEE 802.3 CRC32 checksum is calculated over the payload ($9 + K + V$ bytes).
   * Checksum is written at offset 0 (first 4 bytes).
6. **Sequential Disk Append:** `FileChannel.write(buffer)` streams the frame to disk. If `syncWalOnWrite == true`, `channel.force(false)` is invoked to commit dirty pages to flash storage.
7. **MemTable Insertion:** `activeMemTable.put(key, value, seq)` inserts the entry into `ConcurrentSkipListMap<ByteKey, Record>`.
8. **Heap Memory Accounting:**
   $$\Delta \text{Memory} = \text{Record.ESTIMATED\_OVERHEAD\_BYTES} + K + V$$
   `MemoryTracker.add(delta)` atomically updates live memory tracking.
9. **Return to Caller:** `writeLock` is released and the client receives confirmation in $O(\log N)$ memory time.

---

## 3. Workflow 2: The Tiered Read Path (`get`)

Queries evaluate tiers from newest memory state down to oldest on-disk SSTables:

```
[Client] ──> engine.get(key)
                │
                ├──> Step 1: Query Active MemTable (RAM)
                │        └── Found? ──> Return value (or null if tombstone)
                │
                ├──> Step 2: Query Immutable MemTable (RAM)
                │        └── Found? ──> Return value (or null if tombstone)
                │
                └──> Step 3: Iterate On-Disk SSTables (Newest -> Oldest)
                         │
                         ├── 3a. Bloom Filter Check
                         │       └── Returns false? ──> SKIP SSTABLE (0 Disk I/O!)
                         │
                         ├── 3b. Sparse Index Binary Search
                         │       └── Locate bounded seek range [startOffset, endOffset]
                         │
                         ├── 3c. Positional Disk Read
                         │       └── FileChannel.read(buffer, startOffset)
                         │
                         └── 3d. Sequential Block Scan
                                 ├── Key matches? Verify CRC32 -> Return value
                                 └── Record key > Target key? Stop scan -> Check next SSTable
```

### Granular Execution Steps
1. **Active MemTable Probe:** Lock-free search in `ConcurrentSkipListMap`. If present:
   * If `RecordType.DELETE`: returns `null` immediately (tombstone shadows older records).
   * If `RecordType.PUT`: returns payload bytes.
2. **Immutable MemTable Probe:** If an asynchronous flush is running, checks `immutableMemTable.get()`. If present, returns result.
3. **On-Disk SSTables Traversal:** Iterates through `CopyOnWriteArrayList<SSTableReader> sstables` (ordered from newest table ID to oldest table ID):
   * **Step 3a (Bloom Filter Check):** Invokes `bloomFilter.mightContain(key)`.
     * If `false`: **Key is guaranteed absent.** The engine skips the file completely without performing any disk I/O.
   * **Step 3b (Sparse Index Binary Search):** Invokes `sparseIndex.findSeekRange(key, dataBlockEndOffset)`.
     * Performs `Collections.binarySearch` on in-memory sampled index keys.
     * Determines the exact bounding byte offsets `[startOffset, endOffset]`.
   * **Step 3c (Thread-Safe Positional Disk Read):**
     * Allocates a buffer of size `(endOffset - startOffset)`.
     * Invokes `channel.read(buffer, startOffset)`. This is a positional read that does not modify channel state and is completely thread-safe.
   * **Step 3d (Block Scan & Verification):**
     * Scans records sequentially within the read slice.
     * Compares each candidate key with target key using `Arrays.compareUnsigned`.
     * If exact match: verifies CRC32 checksum. Returns value if `PUT`, or returns `null` if `DELETE`.
     * If candidate key is lexicographically greater than target key: stops searching this SSTable (records are strictly sorted, so target key cannot appear later).
4. **Exhaustion:** If all SSTables are checked without a match, returns `null`.

---

## 4. Workflow 3: The Asynchronous MemTable Flush Pipeline

When in-memory capacity is reached, memory freezes and flushes to disk in the background:

```
[MemTable Threshold Exceeded]
                │
                ▼
[Step 1: Freeze & Rotate]
  ├── activeMemTable.freeze()
  ├── immutableMemTable.set(activeMemTable)
  ├── Close old WAL (wal-0001.log)
  ├── Open new WAL (wal-0002.log)
  └── Allocate fresh activeMemTable
                │
                ▼
[Step 2: Dispatch to Background Worker]
  └── flushExecutor.submit(...)
                │
                ▼
[Step 3: SSTableWriter Serialization]
  ├── 3a. Write Data Block: Contiguous sorted records
  ├── 3b. Sample Sparse Index: Record (Key, Offset) every N entries
  ├── 3c. Populate Bloom Filter: Hash every key into bitset
  ├── 3d. Write Sparse Index Block to disk
  ├── 3e. Write Bloom Filter Block to disk
  └── 3f. Append 32-Byte Fixed Footer & invoke channel.force(true)
                │
                ▼
[Step 4: Register & Cleanup]
  ├── Open SSTableReader(sstable-0001.db)
  ├── Prepend to sstables list (index 0)
  ├── Delete old backing WAL (wal-0001.log)
  └── Clear immutableMemTable reference (set to null)
```

### Granular Execution Steps
1. **MemTable Freeze:** The engine invokes `activeMemTable.freeze()`. The table transitions to read-only mode and rejects any subsequent write attempts.
2. **Double-Buffering Swap:**
   * `immutableMemTable.set(activeMemTable)` keeps data accessible to readers during the flush.
   * Active WAL file is closed.
   * A new WAL file (`wal-0002.log`) and a new `activeMemTable` are initialized.
   * Incoming client writes immediately resume on the new table without blocking on disk I/O.
3. **Background Task Dispatch:** A flush runnable is submitted to `flushExecutor` (a dedicated single-thread executor).
4. **SSTable Construction (`SSTableWriter`):**
   * Creates `sstable-XXXXX.db`.
   * **Data Block:** Iterates sequentially through `MemTable`'s skip-list. Every entry is serialized into its binary frame and written to disk.
   * **Sparse Index Sampling:** Every $N$-th entry (`indexStride`, e.g., 32 or 64), records `(key, currentByteOffset)`.
   * **Bloom Filter Generation:** Computes MurmurHash3 split hashes for every key and sets corresponding bits in `bitSet`.
   * **Index & Bloom Serialization:** Writes the serialized Sparse Index block, followed by the Bloom Filter block.
   * **Footer Append:** Appends the 32-byte fixed footer containing:
     `[IndexOffset: 8B][IndexLen: 4B][BloomOffset: 8B][BloomLen: 4B][MagicNumber: 8B]`
   * **Fsync:** Invokes `channel.force(true)` to ensure that data, indexes, filter, and inode metadata are committed to physical media.
5. **Registration & Deletion:**
   * Opens `SSTableReader.open(path, tableId)`.
   * Inserts the new reader at index 0 of `CopyOnWriteArrayList<SSTableReader>`.
   * Permanently deletes `wal-0001.log` from disk (data is now durable in the SSTable).
   * Sets `immutableMemTable` to `null`.
   * Checks if `sstables.size() >= config.getCompactionThreshold()`.

---

## 5. Workflow 4: The Streaming Merge-Sort Compaction Pipeline

Compaction prevents unbounded read amplification and space amplification:

```
[Compaction Triggered (Count >= Threshold or Manual)]
                │
                ▼
[Step 1: Snapshot SSTable Readers]
  └── Snapshot tablesToCompact (ordered newest to oldest)
                │
                ▼
[Step 2: Initialize Streaming k-Way Min-Heap]
  ├── For each SSTable: Open SSTableIterator
  └── Insert IteratorHead into PriorityQueue (sorted by Key ASC, ReaderIndex ASC)
                │
                ▼
[Step 3: Streaming Merge Loop]
  ├── Poll top IteratorHead (smallest key)
  ├── Drain all other heads in heap sharing the same key (older versions)
  ├── Keep ONLY newest version (deduplication)
  └── Check Tombstone:
        ├── If bottom tier: PURGE tombstone (drop from output)
        └── If not bottom tier: PRESERVE tombstone
                │
                ▼
[Step 4: Emit Compacted SSTable]
  └── Write sorted, deduplicated entries to sstable-merged.db
                │
                ▼
[Step 5: Atomic Reader Swap]
  ├── Open new SSTableReader
  ├── Atomically remove obsolete readers and insert new reader in sstables list
  └── Delete old sstable-XXXX.db files from disk (reclaiming space)
```

### Granular Execution Steps
1. **Trigger Condition:** Compaction runs when `sstables.size() >= config.getCompactionThreshold()` or when `engine.compact()` is invoked explicitly.
2. **Reader Snapshot:** Obtains a snapshot of all candidate `SSTableReader` instances.
3. **Heap Initialization:**
   * For each reader, instantiates an `SSTableIterator`.
   * Wraps each iterator in an `IteratorHead(readerIndex, iterator)` where lower `readerIndex` indicates a newer table.
   * Pushes all heads into a `PriorityQueue<IteratorHead>`.
4. **Streaming Deduplication & Tombstone GC:**
   * Pulls the smallest key `currentKey` from the min-heap.
   * Drains all other entries matching `currentKey` from the heap:
     * The first entry polled is from the lowest `readerIndex` (newest table); this is the latest state.
     * All subsequent entries are older revisions; they are dropped (deduplication).
   * **Tombstone Evaluation:**
     * If the latest state is a tombstone and `isBottomTier == true`, the tombstone is purged completely.
     * Otherwise, the tombstone is retained to shadow older SSTables that were not included in this compaction run.
5. **Disk Emission:** Writes the deduplicated stream to a new SSTable file (`sstable-YYYYY.db`).
6. **Atomic Reader Swap:**
   * Acquires `writeLock`.
   * Removes all compacted readers from `CopyOnWriteArrayList<SSTableReader>`.
   * Inserts the new reader and sorts by table ID descending.
   * Releases `writeLock`.
7. **Disk Space Reclamation:** Closes and deletes the old input `.db` files from disk.

---

## 6. Workflow 5: Bootstrapping & Crash Recovery Pipeline

When the engine restarts, it reconstructs its complete state from disk:

```
[Engine Boot: LsmEngine.open(config)]
                │
                ▼
[Phase 1: Discover Existing SSTables]
  ├── Scan dataDir/sstable/ for "sstable-*.db"
  ├── For each file: Read 32-byte footer from EOF
  ├── Load Sparse Index and Bloom Filter into RAM
  └── Sort readers newest-first and set tableIdGenerator
                │
                ▼
[Phase 2: Replay Write-Ahead Logs (WAL)]
  ├── Scan dataDir/wal/ for "wal-*.log" (sorted chronologically)
  ├── For each WAL file: WalRecovery.recover(path)
  │     ├── Read 13-byte headers sequentially
  │     ├── Verify CRC32 checksum of each frame
  │     ├── On EOF partial frame / CRC error: TRUNCATE file to last valid offset
  │     └── Populate recovered records into in-memory table
  └── Synchronize sequenceNumberGenerator
                │
                ▼
[Phase 3: State Initialization]
  ├── If recovered table > threshold: Immediately flush to new SSTable
  ├── If recovered table <= threshold: Adopt into active MemTable & rewrite to active WAL
  └── Initialize active WriteAheadLog (wal-XXXXX.log) -> READY FOR TRAFFIC
```

---

## 7. Testing & Verification Playbook

### Running All Unit, Integration, and Concurrency Tests
Execute the full test suite in Maven:
```bash
mvn clean test
```

### Running the Empirical Performance Micro-Benchmark
Run the standalone performance benchmark to measure write IOPS, p50/p95/p99 read seek latencies, and Bloom filter avoidance ratios:
```bash
mvn test -Dtest=LsmPerformanceBenchmark
```

#### Typical Benchmark Output (Apple Silicon / Fast NVMe SSD)
```
================================================================================
       LSM STORAGE ENGINE EMPIRICAL PERFORMANCE MICRO-BENCHMARK
================================================================================
Storage Path:            /var/folders/temp/lsm-bench-1718291029
MemTable Flush Boundary: 524,288 bytes
Sparse Index Stride:     32 records
Bloom Filter FP Rate:    1.00%
--------------------------------------------------------------------------------
Executing 50,000 sequential Put operations...
-> Write Duration:       0.386 seconds
-> Write Throughput:     129,684 ops/sec
-> On-Disk SSTables:     18 files
--------------------------------------------------------------------------------
Executing 20,000 random Read Point Queries (Hits)...
-> Point Query (Hit) p50 Latency:  3.38 µs
-> Point Query (Hit) p95 Latency:  5.63 µs
-> Point Query (Hit) p99 Latency:  12.21 µs
-> Point Query (Hit) Max Latency:  637.46 µs
--------------------------------------------------------------------------------
Executing 20,000 random Read Point Queries (Misses / Absent Keys)...
-> Point Query (Miss) p50 Latency: 2.00 µs
-> Point Query (Miss) p95 Latency: 3.25 µs
-> Point Query (Miss) p99 Latency: 3.88 µs
-> Bloom Filter Suppressed Checks: 533,149
--------------------------------------------------------------------------------
Executing Full Compaction across active SSTables...
-> Compaction Duration:           0.147 seconds
-> Compacted SSTables:            18 files -> 1 file
-> Reclaimed Disk Space:          1,330 bytes
================================================================================
```
