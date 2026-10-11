# LSM Tree: Theoretical Foundations, Architecture, and Systems Design

A comprehensive, engineering-grade reference on Log-Structured Merge (LSM) storage engines, on-disk binary layouts, probabilistic data structures, and memory-to-disk persistence pipelines.

---

## Table of Contents
1. [The Storage Engine Dilemma: Why LSM Trees Exist](#1-the-storage-engine-dilemma-why-lsm-trees-exist)
2. [Theoretical Framework: The RUM Conjecture](#2-theoretical-framework-the-rum-conjecture)
3. [Deep Architectural Breakdown](#3-deep-architectural-breakdown)
   - [3.1 The Write-Ahead Log (WAL) & Durability Semantics](#31-the-write-ahead-log-wal--durability-semantics)
   - [3.2 The In-Memory MemTable & Heap Accounting](#32-the-in-memory-memtable--heap-accounting)
   - [3.3 Sorted String Tables (SSTables) & Physical Binary Layout](#33-sorted-string-tables-sstables--physical-binary-layout)
   - [3.4 Sparse Indexing: Mechanics & Math](#34-sparse-indexing-mechanics--math)
   - [3.5 Bloom Filters: Mathematical Optimization & Hashing](#35-bloom-filters-mathematical-optimization--hashing)
   - [3.6 The Deletion Dilemma: Tombstones & Ghost Data](#36-the-deletion-dilemma-tombstones--ghost-data)
   - [3.7 Compaction Strategies: Merging, Deduplication, & GC](#37-compaction-strategies-merging-deduplication--gc)
4. [Concurrency, Crash Consistency, & POSIX File Semantics](#4-concurrency-crash-consistency--posix-file-semantics)
5. [Primary Research, Production References, and Web Links](#5-primary-research-production-references-and-web-links)

---

## 1. The Storage Engine Dilemma: Why LSM Trees Exist

### 1.1 The Inherent Bottleneck of $B^+$-Trees in Write-Heavy Systems
Traditional relational database storage engines (such as MySQL InnoDB and PostgreSQL) organize persistent data using variations of the **$B^+$-Tree**. In a $B^+$-Tree:
* Records are stored in fixed-size blocks (typically 8 KB or 16 KB pages).
* An update or insert requires locating the exact page containing the record's key and modifying that page **in place**.
* Because keys arrive in arbitrary order, writes scatter across random disk pages, causing **Random Disk I/O**.

```
Traditional B+ Tree (In-Place Random Writes):
Incoming Write: [Key: "zebra", Val: "100"] 
       │
       ▼ (Traverse Tree Index)
Disk: [Page 12]  [Page 84]  [Page 302] ──> Overwrites 16 KB Page 302 on disk in-place!
```

### 1.2 The Hardware Reality: Spinning Disks vs. Solid State Drives (SSDs)
1. **Mechanical Hard Drives (HDDs):** Random writes force physical actuator arms to seek across rotating platters (5–10 ms per seek). A disk that delivers 150 MB/s sequential throughput collapses to ~1 MB/s under random writes.
2. **Solid State Drives (NAND Flash):** While SSDs lack moving parts and handle random reads well, they cannot overwrite flash cells in place. NAND flash must be erased in large blocks (typically 2 MB to 8 MB) before individual pages (4 KB to 16 KB) can be rewritten.
   * **Write Amplification (WA):** Updating a 100-byte record in an 8 KB page requires writing the entire 8 KB page, and later forces the SSD's Flash Translation Layer (FTL) to relocate and erase multi-megabyte flash blocks during garbage collection.
   * **Wear-Out:** Flash memory endurance is limited (typically 1,000 to 10,000 Program/Erase cycles). Random writes accelerate drive wear.

### 1.3 The LSM Paradigm Shift: All Writes Are Sequential
The **Log-Structured Merge-Tree** (originally formalized by Patrick O'Neil, Edward O'Neil, and Gerhard Weikum in 1996) eliminates random disk writes entirely.
* In an LSM engine, updates and inserts are **never written in place**.
* All inbound writes are buffered in high-speed RAM (**MemTable**) and sequentially appended to a binary **Write-Ahead Log (WAL)**.
* When memory fills, sorted records are flushed sequentially to immutable on-disk files (**Sorted String Tables / SSTables**).
* Sequential writes achieve maximum hardware throughput (up to 3,000+ MB/s on modern NVMe drives) with zero SSD block re-erase churn.

---

## 2. Theoretical Framework: The RUM Conjecture

When designing database storage engines, the **RUM Conjecture** (formalized by Manos Athanassoulis, Stratos Idreos, et al. at Harvard University) defines the fundamental trade-offs between three competing dimensions:
* **R:** Read Overhead (Read Amplification & Seek Latency)
* **U:** Update Overhead (Write Amplification & Throughput)
* **M:** Memory/Space Overhead (Space Amplification & RAM utilization)

$$\text{Read Overhead} \times \text{Update Overhead} \times \text{Memory Overhead} \ge \text{Constant}$$

You can optimize for any two dimensions, but only at the expense of the third.

| Dimension | $B^+$-Tree (e.g., MySQL InnoDB) | LSM-Tree (e.g., RocksDB, Cassandra, Our Engine) | Bitcask / Log-Structured Hash |
| :--- | :--- | :--- | :--- |
| **Write Throughput** | **Low:** Random I/O, page splits, in-place page writes. | **Extremely High:** Sequential appends to WAL and sequential SSTable flushes. | **Extremely High:** Append-only log. |
| **Write Amplification (WA)** | High (rewriting full pages for small byte deltas). | Low at ingestion; moderate during multi-pass compaction. | Minimal (pure append). |
| **Point Read Latency** | **Extremely Low & Deterministic:** Traverses $B^+$-tree directly to the single leaf page. | **Variable:** Checks MemTable $\rightarrow$ Bloom Filter $\rightarrow$ Sparse Index $\rightarrow$ SSTable. | **Extremely Low:** Direct in-memory hash table seek. |
| **Space Amplification (SA)** | Moderate (page fragmentation leaves 20%–40% slack space). | Low after compaction; temporarily elevated before compaction. | High (obsolete keys consume log space until compaction). |
| **Range Queries** | **Optimal:** Sequential traversal across linked leaf pages. | Good: Multi-way iterator merge across SSTables. | Poor: Requires sorting entire hash index. |

---

## 3. Deep Architectural Breakdown

```
WRITE PATH:
Client Write (Key, Value)
     │
     ├───> 1. Write-Ahead Log (WAL)  [Disk: Sequential FileChannel.write, fsync]
     │
     └───> 2. Active MemTable        [RAM: ConcurrentSkipListMap, AtomicMemoryTracker]
                │
           (When MemTable hits threshold: 4MB / 32MB)
                │
                ▼
           3. Immutable MemTable     [RAM: Read-Only Flush Buffer]
                │
                ▼ (Background Flush Thread)
           4. Sorted String Table    [Disk: sstable-XXXXX.db]
                ├── Data Block       [Contiguous [CRC][Type][KLen][VLen][Key][Val]]
                ├── Sparse Index     [Sampled (Key, Offset) entries for binary search]
                ├── Bloom Filter     [Murmur3-hash Kirsch-Mitzenmacher bitset]
                └── 32-Byte Footer   [Offsets to Index, Bloom, Magic Number]
```

---

### 3.1 The Write-Ahead Log (WAL) & Durability Semantics

Because the MemTable resides in volatile RAM, a server crash, OS kernel panic, or sudden power loss would lose buffered writes. To satisfy the **Durability (D)** in ACID:
1. Every write operation is serialized into a binary frame and written to disk **before** the in-memory MemTable is modified.
2. The WAL file is strictly append-only.

#### Binary Frame Layout
Each record is serialized with a 13-byte fixed header plus variable payloads:

```
+----------------+---------------+----------------+----------------+----------------+------------------+
| CRC32 Checksum | Record Type   | Key Length (K) | Val Length (V) | Key Bytes      | Value Bytes      |
| 4 Bytes        | 1 Byte        | 4 Bytes        | 4 Bytes        | K Bytes        | V Bytes          |
+----------------+---------------+----------------+----------------+----------------+------------------+
|<----------------- 13 Bytes Fixed Metadata Header --------------->|<------- Variable Payload -------->|
```

* **CRC32 (4 Bytes):** Computed using the standard IEEE 802.3 polynomial ($0xEDB88320$). The checksum covers the entire payload: `[RecordType][KeyLen][ValLen][Key][Val]`. This detects torn writes (partial disk flushes during crashes) and storage media bit-rot.
* **Record Type (1 Byte):**
  * `0x01 = PUT` (Active key-value mutation)
  * `0x02 = DELETE` (Tombstone marker)
* **Key Length (4 Bytes):** 32-bit signed integer defining the key size in bytes.
* **Value Length (4 Bytes):** 32-bit signed integer defining the value size in bytes (always `0` for tombstones).

#### Durability Modes: OS Page Cache vs. `fsync`
* **Buffered Writes (`syncOnWrite = false`):** The engine writes to Java NIO `FileChannel`. The OS buffers the bytes in the kernel page cache. Throughput is high (>120,000 ops/sec), but data could be lost if the physical machine loses power before the OS flushes dirty pages.
* **Synchronous Writes (`syncOnWrite = true`):** The engine invokes `channel.force(false)`, triggering a POSIX `fdatasync()` / `fsync()` syscall. The drive flushes internal volatile caches to persistent NAND flash. Throughput is bound by disk rotation or SSD controller sync latency (~1,000 to 10,000 ops/sec).

---

### 3.2 The In-Memory MemTable & Heap Accounting

The MemTable buffers writes in sorted key order.

#### Why `ConcurrentSkipListMap` Instead of Red-Black Trees (`TreeMap`)?
* A standard Red-Black Tree (`java.util.TreeMap`) requires structural node rebalancing (color flips and rotations) upon mutation. Rebalancing touches multiple pointers across the tree, requiring coarse-grained synchronization or heavy read-write locks (`ReentrantReadWriteLock`).
* A **Skip-List** (invented by William Pugh in 1990) is a probabilistic multi-level linked list. It allows lock-free concurrent updates using single-word Atomic Compare-And-Swap (CAS) operations. Multiple reader threads can traverse the index towers simultaneously without blocking or being blocked by write threads.

```
Level 3:  [Header] ───────────────────────────> [Key: "m"] ───────────────────────────> null
Level 2:  [Header] ────────────> [Key: "f"] ──> [Key: "m"] ────────────> [Key: "s"] ──> null
Level 1:  [Header] ──> [Key: "c"] -> [Key: "f"] -> [Key: "m"] -> [Key: "p"] -> [Key: "s"] -> null
```

#### Atomic Heap Memory Accounting
To prevent JVM `OutOfMemoryError` (OOM), the engine tracks memory occupancy dynamically via `MemoryTracker`. On a 64-bit JVM with compressed oops:
* `Record` object header + 4 references + `long` sequenceNumber: ~48 bytes.
* `ByteKey` object header + reference + `int` hashCode: ~32 bytes.
* `byte[]` arrays (key and value): 16-byte array header + 4-byte length + 4-byte alignment + payload length.
* `ConcurrentSkipListMap.Node` overhead (internal `volatile` pointers and index towers): ~48 bytes.
* **Total Estimated Overhead:** `Record.ESTIMATED_OVERHEAD_BYTES = 128` bytes plus `key.length` plus `value.length`.

When `activeMemTable.getByteSize() >= config.getMemTableThresholdBytes()`, the engine rotates the table.

---

### 3.3 Sorted String Tables (SSTables) & Physical Binary Layout

An SSTable is an immutable disk file containing sorted records.

```
+-------------------------------------------------------------------------------+
| SECTION 1: DATA BLOCK (Contiguous Sorted Records)                            |
| Record 0: [CRC32: 4B][Type: 1B][KeyLen: 4B][ValLen: 4B][Key Bytes][Val Bytes] |
| Record 1: [CRC32: 4B][Type: 1B][KeyLen: 4B][ValLen: 4B][Key Bytes][Val Bytes] |
| ...                                                                           |
| Record N: [CRC32: 4B][Type: 1B][KeyLen: 4B][ValLen: 4B][Key Bytes][Val Bytes] |
+-------------------------------------------------------------------------------+
| SECTION 2: SPARSE INDEX BLOCK                                                 |
| [EntryCount: 4B]                                                              |
| Entry 0: [KeyLen: 4B][Key Bytes][FileOffset: 8B]                              |
| Entry 1: [KeyLen: 4B][Key Bytes][FileOffset: 8B]                              |
| ...                                                                           |
+-------------------------------------------------------------------------------+
| SECTION 3: BLOOM FILTER BLOCK                                                 |
| [BitSize: 4B][NumHashFunctions: 4B][LongArrayLen: 4B][BitSet Words: L * 8B]   |
+-------------------------------------------------------------------------------+
| SECTION 4: FIXED 32-BYTE FOOTER (Appended at EOF)                             |
| [IndexOffset: 8B][IndexLen: 4B][BloomOffset: 8B][BloomLen: 4B][Magic: 8B]     |
+-------------------------------------------------------------------------------+
```

#### The 32-Byte Fixed Footer
Because the data block, index block, and bloom filter block are variable in size, reading the file would normally require parsing from byte 0. The fixed footer solves this:
1. When opening `sstable-0000000001.db`, `SSTableReader` seeks directly to `fileSize - 32`.
2. It deserializes the footer and verifies the 8-byte magic number (`0x4C534D5452454531L`, ASCII for `LSMTREE1`).
3. In $O(1)$ time, it reads `indexLength` bytes from `indexOffset` to load the Sparse Index, and `bloomFilterLength` bytes from `bloomFilterOffset` to load the Bloom filter into RAM.

---

### 3.4 Sparse Indexing: Mechanics & Math

A **dense index** indexes every key in the database, consuming excessive RAM.
A **sparse index** samples only every $N$-th key (configured by `indexStride`, e.g., 32 or 64).

```
Sparse Index in RAM:
[ "apple" -> @0 ]   [ "cat" -> @1024 ]   [ "dog" -> @2180 ]   [ "fox" -> @3200 ]
```

When querying `targetKey = "cow"`:
1. Binary search (`Collections.binarySearch`) on the in-memory sparse index determines that `"cat" < "cow" < "dog"`.
2. The seek range is bounded to `[1024, 2180]`.
3. The engine performs a single contiguous disk read of $2180 - 1024 = 1156$ bytes, scanning only that small segment.
4. If `"cow"` exists, it is found. If the scan reaches `"dog"` without finding `"cow"`, the query terminates immediately because records are strictly sorted.

---

### 3.5 Bloom Filters: Mathematical Optimization & Hashing

A **Bloom filter** (invented by Burton Howard Bloom in 1970) is a space-efficient probabilistic data structure that answers set membership queries:
* **Returns `false`:** The element is **definitely not** in the set. (Zero False Negatives guaranteed).
* **Returns `true`:** The element is **probably** in the set (with a bounded false positive probability $p$).

#### Mathematical Sizing Proofs
Given $n$ expected keys and target false positive probability $p$:
1. Optimal bit array size $m$:
$$m = -\frac{n \ln p}{(\ln 2)^2}$$

2. Optimal number of hash functions $k$:
$$k = \frac{m}{n} \ln 2$$

For $p = 0.01$ (1% false positive rate):
$$\frac{m}{n} \approx 9.58 \text{ bits per key}, \quad k \approx 7 \text{ hash functions}$$

Storing 1,000,000 keys requires only ~1.14 MB of RAM for the Bloom filter while eliminating 99% of unnecessary disk seeks!

#### The Kirsch-Mitzenmacher Optimization
Computing $k$ independent cryptographic or Murmur hashes is computationally expensive. We implement the Kirsch-Mitzenmacher theorem (Harvard, 2006):
$$g_i(x) = h_1(x) + i \cdot h_2(x) \pmod m$$
By generating just two 64-bit MurmurHash3 split values ($h_1$ and $h_2$), we simulate any number of hash functions $k$ with simple integer arithmetic.

---

### 3.6 The Deletion Dilemma: Tombstones & Ghost Data

In an append-only immutable storage system, you cannot reach into an on-disk SSTable to delete bytes.
* Deleting a key writes a **Tombstone** (`RecordType.DELETE`, `value = byte[0]`).
* The tombstone travels through the WAL and MemTable just like an ordinary write.
* During point queries, encountering a tombstone indicates that the key was deleted. Older values in older SSTables are ignored.
* **Tombstone Shadowing:** A tombstone in a newer SSTable shadows and hides historical values of that key in older SSTables.
* **Tombstone Garbage Collection:** A tombstone cannot be removed until compaction merges it with the oldest SSTable in the engine. If a tombstone were prematurely deleted while older SSTables still held the key, the deleted key would resurrect!

---

### 3.7 Compaction Strategies: Merging, Deduplication, & GC

As writes accumulate, multiple SSTables collect on disk. This causes:
1. **Read Amplification:** Queries must check multiple SSTables.
2. **Space Amplification:** Multiple superseded versions of updated keys and tombstones consume disk space.

Compaction solves this by executing a **streaming multi-way ($k$-way) merge-sort**.

```
Input SSTable 1: [ "k1": "v1_old" ]  [ "k2": "v2_old" ]  [ "k3": "v3" ]
Input SSTable 2: [ "k1": "v1_new" ]  [ "k2": TOMBSTONE ] [ "k4": "v4" ]
                                      │
                                      ▼ (Streaming Merge-Sort)
Compacted SSTable: [ "k1": "v1_new" ]  [ "k3": "v3" ]  [ "k4": "v4" ]
                   (k1 updated; k2 purged if bottom tier)
```

#### Compaction Mechanics
1. An iterator is opened over each input SSTable.
2. An `IteratorHead` for each table is added to a `PriorityQueue` (min-heap) ordered by `ByteKey` ascending.
3. If identical keys exist across multiple tables, the record from the newest SSTable is selected; older versions are discarded.
4. If the latest record is a Tombstone:
   * If this compaction includes the oldest SSTable in the engine (`isBottomTier = true`), the tombstone is purged.
   * If older SSTables remain below this set, the tombstone is preserved to continue shadowing older records.
5. The merged stream is written to a new SSTable (`sstable-merged.db`).
6. The engine atomically swaps the new reader into the active reader list and unlinks the obsolete `.db` files from disk.

---

## 4. Concurrency, Crash Consistency, & POSIX File Semantics

### 4.1 Positional Thread-Safe I/O
Standard `FileChannel.position(pos)` and `FileChannel.read(buf)` update internal channel state. If multiple threads access the same `FileChannel` concurrently, race conditions occur.
Our implementation uses **positional reads**:
```java
channel.read(ByteBuffer dst, long position);
```
`channel.read(dst, position)` operates atomically at the OS kernel level (equivalent to the POSIX `pread()` syscall). Multiple reader threads can query the same SSTable concurrently without locks.

### 4.2 Crash Recovery Guarantees
* If a machine crashes mid-write, the WAL file may contain a partial frame at its tail.
* On reboot, `WalRecovery` reads sequentially from byte 0, validating CRC32 checksums.
* When it encounters a partial frame or CRC mismatch at EOF, it invokes `channel.truncate(lastValidOffset)`.
* Every confirmed write is recovered; torn frames are safely discarded.

---

## 5. Primary Research, Production References, and Web Links

Foundational literature, research papers, and production source code referenced in this engine:

### Foundational Research Papers
* **The Log-Structured Merge-Tree (LSM-Tree):** Patrick O'Neil, Edward O'Neil, Gerhard Weikum (1996).  
  Link: [ACM Digital Library: The Log-Structured Merge-Tree (LSM-Tree)](https://dl.acm.org/doi/10.1007/s007780050022)
* **Bigtable: A Distributed Storage System for Structured Data:** Fay Chang, Jeffrey Dean, Sanjay Ghemawat, et al. (Google, OSDI 2006).  
  Link: [Google Research: Bigtable Paper](https://research.google/pubs/pub27898/)
* **The RUM Conjecture: Designing Read-Update-Memory Optimized Access Paths:** Manos Athanassoulis, Stratos Idreos, et al. (Harvard University, EDBT 2016).  
  Link: [Harvard DASlab: Designing Access Methods: The RUM Conjecture](https://stratos.seas.harvard.edu/files/stratos/files/rum.pdf)
* **Less Hashing, Same Performance: Building a Better Bloom Filter:** Adam Kirsch, Michael Mitzenmacher (Harvard University, ESA 2006).  
  Link: [Harvard CRCS: Building a Better Bloom Filter](https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf)
* **Skip Lists: A Probabilistic Alternative to Balanced Trees:** William Pugh (Communications of the ACM, 1990).  
  Link: [Pugh Skip Lists Paper (PDF)](https://epaperpress.com/sortsearch/download/skiplist.pdf)

### Production Storage Engines & Source Repositories
* **RocksDB Architecture Guide (Meta):**  
  Link: [RocksDB GitHub Wiki: Architecture Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)
* **Apache Cassandra SSTable Design:**  
  Link: [Apache Cassandra Documentation: SSTable Architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage_engine.html)
* **Google LevelDB Source & Overview:**  
  Link: [LevelDB GitHub Repository](https://github.com/google/leveldb)
* **TiKV Storage Engine Design (PingCAP):**  
  Link: [TiKV Architecture Documentation](https://tikv.org/deep-dive/key-value-engine/b-tree-vs-lsm/)

### Java NIO, Networking & Algorithms
* **Java 21 `FileChannel` API Specification:**  
  Link: [Oracle Java 21 Docs: FileChannel](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/nio/channels/FileChannel.html)
* **Java 21 `ConcurrentSkipListMap` Specification:**  
  Link: [Oracle Java 21 Docs: ConcurrentSkipListMap](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ConcurrentSkipListMap.html)
* **MurmurHash3 Specification & Implementation (Austin Appleby):**  
  Link: [SMHasher & MurmurHash3 Repository](https://github.com/aappleby/smhasher)
