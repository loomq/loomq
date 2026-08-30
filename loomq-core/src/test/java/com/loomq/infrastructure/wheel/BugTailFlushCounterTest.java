package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * C18-4(r18): TailIndex flush 单调计数器(WheelStore.Bucket P1-4 模式镜像)。
 * 红形态声明:旧实现无计数器缝(hasUnflushed 不存在)→ 结构性红(编译失败即红)。
 * lost-update 在 JVM 内不可黑盒观测(未 force 字节同进程页缓存可见,仅进程死亡可验)——
 * crash 级耐久集成测试列为非目标,正确性由 check+force+publish 原子性程序序论证 +
 * P1-4 已验证模式背书。本测试锁三段不变量 + 并发压强的终态齐平不变量。
 */
class BugTailFlushCounterTest {

    @TempDir Path tmp;

    /** 超 day 视界(>30d)的 tail 域 Intent。 */
    private static Intent tailIntent(String id) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.now().plusMillis(40L * 24 * 3600 * 1000));
        it.setPrecisionTier(PrecisionTier.STANDARD);
        return it;
    }

    @Test
    void counterInvariants() {
        try (TailIndex tail = new TailIndex(tmp, System::currentTimeMillis)) {
            assertFalse(tail.hasUnflushed(), "初始无未刷写");
            tail.put(tailIntent("tail-flush-inv-0001"));
            assertTrue(tail.hasUnflushed(), "append 后必须存在未 force 覆盖的写");
            tail.flush();
            assertFalse(tail.hasUnflushed(), "flush 后计数齐平");
            // 空 flush:快路径无锁返回,计数不动(hasUnflushed 保持 false)——
            // "无写跳过 syscall" 的计数表述(旧实现同义为 dirty==false)
            tail.flush();
            assertFalse(tail.hasUnflushed(), "空 flush 不推进计数(快路径保持齐平)");
        }
    }

    @Test
    void concurrentAppendAndFlushMustNotLoseUpdates() throws Exception {
        try (TailIndex tail = new TailIndex(tmp, System::currentTimeMillis)) {
            int total = 2_000;
            AtomicBoolean stop = new AtomicBoolean(false);
            CountDownLatch writerDone = new CountDownLatch(1);
            Thread writer = new Thread(() -> {
                try {
                    for (int i = 0; i < total; i++) {
                        tail.put(tailIntent("tail-flush-con-" + i));
                        if ((i & 0x3F) == 0) {
                            Thread.yield();   // 微让步,放大 flush/append 交错
                        }
                    }
                } finally {
                    writerDone.countDown();
                }
            });
            Thread flusher = new Thread(() -> {
                while (!stop.get()) {
                    tail.flush();
                }
            });
            flusher.start();
            writer.start();
            assertTrue(writerDone.await(60, TimeUnit.SECONDS), "写者未在时限内完成");
            stop.set(true);
            flusher.join(10_000);
            tail.flush();   // 终 flush:补齐 stop 采样窗口之后的尾部 append
            assertFalse(tail.hasUnflushed(),
                "并发 append×flush 终态必须齐平(lost-update 不变量:任何已完成 append 迟早被 force 覆盖)");
        }
    }
}
