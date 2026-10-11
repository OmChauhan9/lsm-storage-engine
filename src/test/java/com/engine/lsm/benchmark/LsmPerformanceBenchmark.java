package com.engine.lsm.benchmark;

import com.engine.lsm.Config;
import com.engine.lsm.LsmEngine;
import com.engine.lsm.sstable.SSTableReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Empirical micro-benchmark measuring write throughput (ops/sec) and
 * read latency distributions (p50, p95, p99) under sequential and tiered access.
 */
public class LsmPerformanceBenchmark {

    public static void main(String[] args) throws IOException {
        Path tempDir = Path.of(System.getProperty("java.io.tmpdir"), "lsm-bench-" + System.currentTimeMillis());
        runFullBenchmarkSuite(tempDir);
    }

    @Test
    @DisplayName("Execute Empirical Performance Benchmark Suite")
    void testRunBenchmarkSuite(@TempDir Path tempDir) throws IOException {
        runFullBenchmarkSuite(tempDir);
    }

    public static void runFullBenchmarkSuite(Path benchDir) throws IOException {
        int writeOperations = 50_000;
        int readOperations = 20_000;

        Config config = Config.builder(benchDir)
                .memTableThresholdBytes(512 * 1024) // 512 KB
                .indexStride(32)
                .bloomFilterFpRate(0.01)
                .syncWalOnWrite(false)
                .compactionThreshold(100) // Keep tables uncompacted until explicit benchmark compaction
                .build();

        System.out.println("================================================================================");
        System.out.println("       LSM STORAGE ENGINE EMPIRICAL PERFORMANCE MICRO-BENCHMARK");
        System.out.println("================================================================================");
        System.out.printf("Storage Path:            %s%n", benchDir);
        System.out.printf("MemTable Flush Boundary: %,d bytes%n", config.getMemTableThresholdBytes());
        System.out.printf("Sparse Index Stride:     %d records%n", config.getIndexStride());
        System.out.printf("Bloom Filter FP Rate:    %.2f%%%n", config.getBloomFilterFpRate() * 100);
        System.out.println("--------------------------------------------------------------------------------");

        try (LsmEngine engine = LsmEngine.open(config)) {
            // -------------------------------------------------------------------------
            // 1. Write Benchmark
            // -------------------------------------------------------------------------
            System.out.printf("Executing %,d sequential Put operations...%n", writeOperations);
            long writeStart = System.nanoTime();

            for (int i = 0; i < writeOperations; i++) {
                String key = "bench:user:" + String.format("%07d", i);
                String val = "payload-data-record-value-blob-" + i;
                engine.put(key, val);
            }
            engine.flush();
            long writeEnd = System.nanoTime();

            double writeDurationSec = (writeEnd - writeStart) / 1_000_000_000.0;
            double writeOpsPerSec = writeOperations / writeDurationSec;

            System.out.printf("-> Write Duration:       %.3f seconds%n", writeDurationSec);
            System.out.printf("-> Write Throughput:     %,.0f ops/sec%n", writeOpsPerSec);
            System.out.printf("-> On-Disk SSTables:     %d files%n", engine.getSstables().size());
            System.out.println("--------------------------------------------------------------------------------");

            // -------------------------------------------------------------------------
            // 2. Read Latency Benchmark (Hits)
            // -------------------------------------------------------------------------
            System.out.printf("Executing %,d random Read Point Queries (Hits)...%n", readOperations);
            List<Long> hitLatenciesNanos = new ArrayList<>(readOperations);

            for (int i = 0; i < readOperations; i++) {
                int randomId = (i * 37) % writeOperations;
                String key = "bench:user:" + String.format("%07d", randomId);

                long start = System.nanoTime();
                byte[] val = engine.get(key.getBytes());
                long latency = System.nanoTime() - start;

                assertNotNull(val);
                hitLatenciesNanos.add(latency);
            }

            Collections.sort(hitLatenciesNanos);
            double p50HitMicros = hitLatenciesNanos.get((int) (readOperations * 0.50)) / 1_000.0;
            double p95HitMicros = hitLatenciesNanos.get((int) (readOperations * 0.95)) / 1_000.0;
            double p99HitMicros = hitLatenciesNanos.get((int) (readOperations * 0.99)) / 1_000.0;
            double maxHitMicros = hitLatenciesNanos.get(readOperations - 1) / 1_000.0;

            System.out.printf("-> Point Query (Hit) p50 Latency:  %.2f µs%n", p50HitMicros);
            System.out.printf("-> Point Query (Hit) p95 Latency:  %.2f µs%n", p95HitMicros);
            System.out.printf("-> Point Query (Hit) p99 Latency:  %.2f µs%n", p99HitMicros);
            System.out.printf("-> Point Query (Hit) Max Latency:  %.2f µs%n", maxHitMicros);
            System.out.println("--------------------------------------------------------------------------------");

            // -------------------------------------------------------------------------
            // 3. Read Latency Benchmark (Misses / Bloom Filter Suppression)
            // -------------------------------------------------------------------------
            System.out.printf("Executing %,d random Read Point Queries (Misses / Absent Keys)...%n", readOperations);
            List<Long> missLatenciesNanos = new ArrayList<>(readOperations);

            for (int i = 0; i < readOperations; i++) {
                String absentKey = "bench:absent:nonexistent:" + (i + 1_000_000);

                long start = System.nanoTime();
                byte[] val = engine.get(absentKey.getBytes());
                long latency = System.nanoTime() - start;

                missLatenciesNanos.add(latency);
            }

            Collections.sort(missLatenciesNanos);
            double p50MissMicros = missLatenciesNanos.get((int) (readOperations * 0.50)) / 1_000.0;
            double p95MissMicros = missLatenciesNanos.get((int) (readOperations * 0.95)) / 1_000.0;
            double p99MissMicros = missLatenciesNanos.get((int) (readOperations * 0.99)) / 1_000.0;

            long totalBloomNegatives = 0;
            long totalDiskSeeks = 0;
            for (SSTableReader reader : engine.getSstables()) {
                totalBloomNegatives += reader.getBloomNegativeChecks();
                totalDiskSeeks += reader.getDiskBlockSeeks();
            }

            System.out.printf("-> Point Query (Miss) p50 Latency: %.2f µs%n", p50MissMicros);
            System.out.printf("-> Point Query (Miss) p95 Latency: %.2f µs%n", p95MissMicros);
            System.out.printf("-> Point Query (Miss) p99 Latency: %.2f µs%n", p99MissMicros);
            System.out.printf("-> Bloom Filter Suppressed Checks: %,d%n", totalBloomNegatives);
            System.out.println("--------------------------------------------------------------------------------");

            // -------------------------------------------------------------------------
            // 4. Compaction Performance
            // -------------------------------------------------------------------------
            System.out.println("Executing Full Compaction across active SSTables...");
            long compStart = System.nanoTime();
            var compResult = engine.compact();
            long compEnd = System.nanoTime();

            double compSec = (compEnd - compStart) / 1_000_000_000.0;
            System.out.printf("-> Compaction Duration:           %.3f seconds%n", compSec);
            System.out.printf("-> Compacted SSTables:            %d files -> 1 file%n", compResult.obsoleteReaders().size());
            System.out.printf("-> Reclaimed Disk Space:          %,d bytes%n", compResult.bytesReclaimed());
            System.out.println("================================================================================");
        }
    }
}
