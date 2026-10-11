# Single-Node LSM Storage Engine Primitive

[![Java](https://img.shields.io/badge/Java-21%2B-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)]()

An append-only, high-performance Key-Value storage engine built from scratch in Java 21, inspired by the internal storage subsystems of **Google Bigtable**, **RocksDB**, and **Apache Cassandra**.

Buffers inbound writes in an in-memory thread-safe `MemTable` backed by an on-disk binary **Write-Ahead Log (WAL)**. Flushes immutable memory segments sequentially to disk as **Sorted String Tables (SSTables)** with contiguous binary blocks, an in-memory **Sparse Index** for bounded binary-search seeks, and an integrated **Bloom Filter** to eliminate unnecessary disk reads. Provides streaming multi-way **Compaction** to deduplicate records and reclaim disk space.

---

## Documentation Index
* **[In-Depth Theory & Architecture (LSM_THEORY_AND_ARCHITECTURE.md)](LSM_THEORY_AND_ARCHITECTURE.md)**: Deep dive into the RUM Conjecture, $B^+$-Tree vs LSM comparisons, on-disk binary layouts, Bloom filter math, and primary research references with web links.
* **[End-to-End Workflow Manual (WORKFLOW_END_TO_END.md)](WORKFLOW_END_TO_END.md)**: Visual Mermaid diagrams, step-by-step byte-level packet flows for writes, reads, flushes, compactions, and crash recovery.

---

## Empirical Benchmark Performance

Measured on Apple Silicon (M-series, NVMe SSD storage) running `mvn test -Dtest=LsmPerformanceBenchmark`:

| Metric | Measured Result | Description |
| :--- | :--- | :--- |
| **Sequential Write Throughput** | **129,684 ops/sec** | Sustained `put()` writes through WAL and MemTable with automatic SSTable flushes. |
| **Point Query (Hit) p50 Latency** | **3.38 µs** | Median read latency through tiered lookup path. |
| **Point Query (Hit) p95 Latency** | **5.63 µs** | 95th percentile read seek latency. |
| **Point Query (Hit) p99 Latency** | **12.21 µs** | 99th percentile read seek latency. |
| **Point Query (Miss) p50 Latency** | **2.00 µs** | Query latency for absent keys (suppressed in RAM by Bloom Filter). |
| **Point Query (Miss) p99 Latency** | **3.88 µs** | 99th percentile latency for absent keys. |
| **Bloom Filter Disk Seek Suppression** | **533,149 seeks** | Number of unnecessary physical disk reads avoided on negative queries. |
| **Full Compaction Duration** | **0.147 seconds** | 18 SSTable files merged, sorted, and deduplicated into 1 clean SSTable. |

---

## Architectural Highlights

```
Write Path:
Inbound Write (Key, Value) 
   │
   ├──> 1. Write-Ahead Log (WAL) [Disk: Sequential Append, FileChannel.write, CRC32]
   │
   └──> 2. MemTable [RAM: ConcurrentSkipListMap, AtomicMemoryTracker]
              │
         (When MemTable fills up: 512KB / 4MB / 32MB)
              │
              ▼
        3. Immutable MemTable [RAM: Read-Only Flush Buffer]
              │
              ▼ (Background Flush Thread)
        4. Sorted String Table (SSTable) [Disk: sstable-XXXXX.db]
              ├── Section 1: Contiguous Data Block (Sorted Binary Frames)
              ├── Section 2: Sparse Index Block (Loaded into RAM)
              ├── Section 3: Bloom Filter Block (Loaded into RAM)
              └── Section 4: Fixed 32-Byte Footer (Magic Number & Offsets)
```

1. **Durability & Crash Recovery:**
   * Append-only binary log framed with `[CRC32: 4B][Type: 1B][KeyLen: 4B][ValLen: 4B][Key][Val]`.
   * Automatic tail truncation detects and discards torn partial frames from sudden power outages.
2. **Lock-Free Concurrency:**
   * In-memory buffer powered by `ConcurrentSkipListMap` with atomic heap memory accounting.
   * Positional disk I/O (`channel.read(buffer, offset)`) enables concurrent reader threads without synchronization bottlenecks.
3. **Probabilistic Read Amplification Suppression:**
   * Kirsch-Mitzenmacher MurmurHash3 Bloom Filter ($g_i(x) = h_1(x) + i \cdot h_2(x) \pmod m$) guarantees zero false negatives and suppresses >99% of negative disk seeks.
4. **Sparse Indexing:**
   * Samples keys every $N$ entries (`indexStride = 32`), reducing RAM footprint by 97% compared to dense indexes while bounding disk seeks to small contiguous byte slices.
5. **Streaming Multi-Way Compaction:**
   * Min-heap `PriorityQueue` executes a streaming $k$-way merge-sort across SSTables, deduplicating updated keys and garbage-collecting tombstones at the bottom tier.

---

## Quickstart Code Example

```java
import com.engine.lsm.Config;
import com.engine.lsm.LsmEngine;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) throws Exception {
        Path dataDir = Path.of("./data");
        Config config = Config.builder(dataDir)
                .memTableThresholdBytes(4 * 1024 * 1024) // 4 MB flush boundary
                .indexStride(32)                         // Sample 1 key every 32 records
                .bloomFilterFpRate(0.01)                 // 1% Bloom FP target
                .syncWalOnWrite(false)                   // Buffered writes
                .compactionThreshold(4)                  // Compact when 4 SSTables accumulate
                .build();

        try (LsmEngine engine = LsmEngine.open(config)) {
            // Write mutations
            engine.put("user:1001", "Alice Smith");
            engine.put("user:1002", "Bob Jones");

            // Point reads
            String val1 = engine.get("user:1001");
            System.out.println("Retrieved: " + val1); // "Alice Smith"

            // Deletions (Tombstones)
            engine.delete("user:1002");
            String val2 = engine.get("user:1002");
            System.out.println("After Delete: " + val2); // null

            // Explicit Flush & Compaction
            engine.flush();
            engine.compact();
        }
    }
}
```

---

## Project Structure

```
├── pom.xml                                   # Maven configuration (Java 21, JUnit 5, JMH)
├── README.md                                 # High-level overview and benchmark results
├── LSM_THEORY_AND_ARCHITECTURE.md           # In-detail theoretical and architectural guide
├── WORKFLOW_END_TO_END.md                    # In-detail end-to-end operational execution manual
└── src
    ├── main/java/com/engine/lsm
    │   ├── Config.java                       # Engine configuration and builder
    │   ├── LsmEngine.java                    # Primary public facade (put, get, delete, flush, compact)
    │   ├── compaction
    │   │   ├── CompactionResult.java         # Compaction metrics record
    │   │   └── Compactor.java                # Streaming k-way merge-sort engine
    │   ├── memtable
    │   │   ├── MemTable.java                 # ConcurrentSkipListMap wrapper with freeze semantics
    │   │   └── MemoryTracker.java            # Atomic byte-level memory tracking
    │   ├── model
    │   │   ├── ByteKey.java                  # Lexicographical unsigned byte comparator
    │   │   ├── Record.java                   # In-memory record representation
    │   │   └── RecordType.java               # Mutation flags (PUT = 0x01, DELETE = 0x02)
    │   ├── sstable
    │   │   ├── BloomFilter.java              # Murmur3 Kirsch-Mitzenmacher bitset
    │   │   ├── IndexEntry.java               # Sparse index entry mapping key to byte offset
    │   │   ├── SparseIndex.java              # In-memory binary search index
    │   │   ├── SSTableFooter.java            # Fixed 32-byte EOF metadata footer
    │   │   ├── SSTableIterator.java          # Streaming data block sequential iterator
    │   │   ├── SSTableReader.java            # Point queries and bounded block seekers
    │   │   └── SSTableWriter.java            # Sequential flush file builder
    │   └── wal
    │       ├── WalRecord.java                # Binary frame serializer (13 + K + V bytes)
    │       ├── WalRecovery.java              # Crash recovery parser with tail truncation
    │       └── WriteAheadLog.java            # Append-only FileChannel logger
    └── test/java/com/engine/lsm
        ├── CompactionTest.java               # Multi-SSTable deduplication and tombstone GC
        ├── ConcurrentLsmEngineTest.java      # Multi-threaded stress testing
        ├── LsmEngineTest.java                # End-to-end CRUD and tiered read path tests
        ├── WalCrashRecoveryTest.java         # Simulates mid-write crashes and torn writes
        ├── benchmark
        │   ├── LsmJmhBenchmark.java          # OpenJDK JMH micro-benchmark suite
        │   └── LsmPerformanceBenchmark.java  # Empirical IOPS and p99 latency runner
        ├── model/ByteKeyTest.java            # Unsigned byte comparison and equality tests
        ├── sstable
        │   ├── BloomFilterTest.java          # Zero false negative & FP bound verification
        │   └── SSTableReadWriteTest.java     # SSTable reader/writer roundtrip tests
        └── wal
            ├── WalRecoveryTest.java          # CRC corruption and torn frame recovery tests
            └── WriteAheadLogTest.java        # Binary framing and disk size validation tests
```

---

## Building and Testing

### 1. Run the Entire Test Suite
Executes all 19 unit, integration, crash recovery, and concurrency tests:
```bash
mvn clean test
```

### 2. Run the Empirical Performance Micro-Benchmark
Measures sustained write throughput and p50/p95/p99 read seek latencies:
```bash
mvn test -Dtest=LsmPerformanceBenchmark
```

### 3. Run Specific Test Suites
```bash
# Verify Crash Recovery with torn tail injection
mvn test -Dtest=WalCrashRecoveryTest

# Verify Compaction and Tombstone garbage collection
mvn test -Dtest=CompactionTest

# Verify High-Concurrency multi-threaded operations
mvn test -Dtest=ConcurrentLsmEngineTest
```
