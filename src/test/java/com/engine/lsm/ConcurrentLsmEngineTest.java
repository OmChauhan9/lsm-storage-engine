package com.engine.lsm;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrentLsmEngineTest {

    @Test
    @DisplayName("High-concurrency stress test with interleaved writers, readers, and automatic flushes")
    void testConcurrentReadWrites(@TempDir Path tempDir) throws Exception {
        Config config = Config.builder(tempDir)
                .memTableThresholdBytes(8 * 1024) // 8KB threshold forces frequent background flushes
                .indexStride(8)
                .build();

        int numWriters = 4;
        int keysPerWriter = 250;
        int totalKeys = numWriters * keysPerWriter;

        ExecutorService executor = Executors.newFixedThreadPool(numWriters + 2);
        CountDownLatch latch = new CountDownLatch(numWriters);
        AtomicInteger writeErrors = new AtomicInteger(0);

        try (LsmEngine engine = LsmEngine.open(config)) {
            // Launch writer threads
            for (int w = 0; w < numWriters; w++) {
                final int writerId = w;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < keysPerWriter; i++) {
                            String key = "thread-" + writerId + "-key-" + i;
                            String val = "val-" + writerId + "-" + i;
                            engine.put(key, val);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        writeErrors.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            // Launch concurrent reader thread
            Future<?> readerTask = executor.submit(() -> {
                while (latch.getCount() > 0) {
                    try {
                        // Read some keys concurrently while writers are active
                        for (int w = 0; w < numWriters; w++) {
                            engine.get("thread-" + w + "-key-0");
                        }
                        Thread.sleep(1);
                    } catch (Exception ignored) {}
                }
            });

            assertTrue(latch.await(30, TimeUnit.SECONDS), "Writers must complete within timeout");
            readerTask.get(5, TimeUnit.SECONDS);

            assertEquals(0, writeErrors.get(), "No write errors should occur under concurrency");

            // Flush all active tables
            engine.flush();

            // Verify all keys were written and can be retrieved accurately
            for (int w = 0; w < numWriters; w++) {
                for (int i = 0; i < keysPerWriter; i++) {
                    String expectedVal = "val-" + w + "-" + i;
                    String actualVal = engine.get("thread-" + w + "-key-" + i);
                    assertEquals(expectedVal, actualVal, "Mismatch on thread-" + w + "-key-" + i);
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
