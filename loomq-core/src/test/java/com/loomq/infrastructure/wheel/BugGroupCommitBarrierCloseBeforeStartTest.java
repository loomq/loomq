package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * GroupCommitBarrier 未启动就 close 时，inlineForceExecutor 也必须被关闭，
 * 否则构造出的平台线程池资源泄漏。
 */
class BugGroupCommitBarrierCloseBeforeStartTest {

    @TempDir Path tmp;

    @Test
    void closeBeforeStartMustShutdownInlineForceExecutor() throws Exception {
        WheelConfig config = new WheelConfig(tmp.toString(), "t", 30, 16, 1, 10_000L,
            60L * 60_000L, 60_000L, null);
        GroupCommitBarrier barrier = new GroupCommitBarrier(
            new WheelStore(config, System::currentTimeMillis),
            new TailIndex(tmp, System::currentTimeMillis),
            1,
            10_000L);

        barrier.close();

        Field f = GroupCommitBarrier.class.getDeclaredField("inlineForceExecutor");
        f.setAccessible(true);
        ExecutorService executor = (ExecutorService) f.get(barrier);
        assertTrue(executor.isShutdown(),
            "inlineForceExecutor must be shut down even if the barrier was never started");
    }
}
