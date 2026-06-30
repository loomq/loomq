package com.loomq.infrastructure.wal;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.loomq.config.WalConfig;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * 并发压力测试：验证 truncateBefore() 在 flushLoop 线程运行时不会崩溃。
 *
 * <p>修复前，truncateBefore() 先调用 arena.close() 再设置 seg.closed = true，
 * flushLoop 可能在中间窗口对已关闭的 mappedRegion 调用 force()，导致 JVM 崩溃。
 * 修复后，truncateBefore() 先设置 seg.closed = true，flushLoop 会跳过已关闭的段。</p>
 *
 * @since v0.9.2
 */
@Tag("slow")
class SimpleWalWriterTruncateTest {

    private Path dataDir;
    private SimpleWalWriter writer;

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
        if (dataDir != null) {
            try {
                // 递归删除临时目录
                Files.walk(dataDir)
                    .sorted(java.util.Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                    });
            } catch (Exception ignored) {}
        }
    }

    /**
     * 并发 truncateBefore + flushLoop 压力测试。
     *
     * <p>使用 1MB 小段快速产生多段，同时在多个线程并发调用 truncateBefore()
     * 清理旧段，验证 flushLoop 线程不会因访问已释放的 arena 而崩溃。</p>
     */
    @Test
    void concurrentTruncateBeforeAndFlushLoop_shouldNotCrash() throws Exception {
        dataDir = Files.createTempDirectory("wal-truncate-stress-");
        // 1MB 段，快速轮转；低刷盘阈值和间隔，让 flushLoop 频繁运行
        WalConfig config = new WalConfig(
            dataDir.toString(), 1, "batch", 10, false, "memory_segment",
            1, 8, 4, 1, 8, 1, false
        );
        writer = new SimpleWalWriter(config, "truncate-stress");
        writer.start();

        AtomicBoolean running = new AtomicBoolean(true);
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        // 写入线程：持续写入数据推动段轮转
        Thread writerThread = Thread.ofVirtual().name("wal-writer").start(() -> {
            try {
                byte[] payload = makePayload(256);
                while (running.get()) {
                    try {
                        writer.writeDurable(payload).join();
                    } catch (Exception e) {
                        if (running.get()) {
                            errors.add(e);
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    errors.add(e);
                }
            }
        });

        // truncateBefore 线程：持续截断旧段
        Thread truncator = Thread.ofVirtual().name("wal-truncator").start(() -> {
            try {
                while (running.get()) {
                    long pos = writer.getWritePosition();
                    // 截断当前位置之前的段（保留当前段）
                    if (pos > 0) {
                        try {
                            writer.truncateBefore(pos);
                        } catch (Exception e) {
                            if (running.get()) {
                                errors.add(e);
                            }
                        }
                    }
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                if (running.get()) {
                    errors.add(e);
                }
            }
        });

        // 运行 3 秒
        Thread.sleep(3000);
        running.set(false);
        writerThread.join(5000);
        truncator.join(5000);

        // 验证没有异常
        assertDoesNotThrow(() -> {
            if (!errors.isEmpty()) {
                throw new AssertionError(
                    "Concurrent truncateBefore + flushLoop caused " + errors.size() + " error(s): "
                        + errors.get(0).getMessage(), errors.get(0));
            }
        }, "truncateBefore should not crash when flushLoop is running");
    }

    /**
     * 多 truncateBefore 线程并发竞争测试。
     *
     * <p>多个线程同时对同一组旧段调用 truncateBefore()，验证线程安全。</p>
     */
    @Test
    void multipleConcurrentTruncators_shouldNotCrash() throws Exception {
        dataDir = Files.createTempDirectory("wal-multi-trunc-");
        WalConfig config = new WalConfig(
            dataDir.toString(), 1, "batch", 10, false, "memory_segment",
            1, 8, 4, 1, 8, 1, false
        );
        writer = new SimpleWalWriter(config, "multi-trunc");
        writer.start();

        AtomicBoolean running = new AtomicBoolean(true);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        int threadCount = 4;

        // 写入线程
        Thread writerThread = Thread.ofVirtual().name("wal-writer").start(() -> {
            try {
                byte[] payload = makePayload(256);
                while (running.get()) {
                    try {
                        writer.writeDurable(payload).join();
                    } catch (Exception e) {
                        if (running.get()) {
                            errors.add(e);
                        }
                    }
                }
            } catch (Exception e) {
                if (running.get()) {
                    errors.add(e);
                }
            }
        });

        // 多个 truncateBefore 线程
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Thread> truncators = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            int id = i;
            Thread t = Thread.ofVirtual().name("truncator-" + id).start(() -> {
                try {
                    barrier.await();
                    while (running.get()) {
                        long pos = writer.getWritePosition();
                        if (pos > 0) {
                            try {
                                writer.truncateBefore(pos);
                            } catch (Exception e) {
                                if (running.get()) {
                                    errors.add(e);
                                }
                            }
                        }
                        Thread.sleep(2);
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        errors.add(e);
                    }
                }
            });
            truncators.add(t);
        }

        // 运行 3 秒
        Thread.sleep(3000);
        running.set(false);
        writerThread.join(5000);
        for (Thread t : truncators) {
            t.join(5000);
        }

        assertDoesNotThrow(() -> {
            if (!errors.isEmpty()) {
                throw new AssertionError(
                    "Multiple concurrent truncators caused " + errors.size() + " error(s): "
                        + errors.get(0).getMessage(), errors.get(0));
            }
        }, "Multiple concurrent truncateBefore calls should not crash");
    }

    private byte[] makePayload(int size) {
        // 创建一个简单的大 payload 用于快速填充 WAL
        Intent intent = new Intent("truncate-test-" + System.nanoTime());
        intent.setPrecisionTier(PrecisionTier.STANDARD);
        intent.transitionTo(IntentStatus.SCHEDULED);
        byte[] encoded = IntentBinaryCodec.encode(intent);
        // 填充到指定大小
        if (encoded.length >= size) {
            return encoded;
        }
        byte[] padded = new byte[size];
        System.arraycopy(encoded, 0, padded, 0, encoded.length);
        return padded;
    }
}
