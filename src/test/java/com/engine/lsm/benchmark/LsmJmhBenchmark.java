package com.engine.lsm.benchmark;

import com.engine.lsm.Config;
import com.engine.lsm.LsmEngine;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
public class LsmJmhBenchmark {

    private Path tempDir;
    private LsmEngine engine;
    private final AtomicLong counter = new AtomicLong(0);

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("lsm-jmh-");
        Config config = Config.builder(tempDir)
                .memTableThresholdBytes(2 * 1024 * 1024)
                .syncWalOnWrite(false)
                .build();
        engine = LsmEngine.open(config);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }

    @Benchmark
    public void benchmarkPuts() throws IOException {
        long id = counter.incrementAndGet();
        engine.put("key:" + id, "val:" + id);
    }

    @Benchmark
    public byte[] benchmarkGets() throws IOException {
        long id = (counter.get() > 0) ? (counter.get() % 1000) : 0;
        return engine.get(("key:" + id).getBytes());
    }
}
