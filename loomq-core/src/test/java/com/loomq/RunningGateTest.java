package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * P2-1 门控语义测试：验证 running 闸门在 start() 末尾才开放，
 * started 闸门防止 start() 重入。
 */
class RunningGateTest {

    @Test
    void createIntentBeforeStartThrows(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("gate-1").build()) {
            assertFalse(engine.isRunning(),
                "running must be false before start()");
            CompletableFuture<Long> future = engine.createIntent(new Intent(), AckMode.DURABLE);
            ExecutionException ex = assertThrows(ExecutionException.class, future::get,
                "createIntent must fail before start()");
            assertTrue(ex.getCause() instanceof IllegalStateException,
                "cause must be IllegalStateException");
        }
    }

    @Test
    void doubleStartThrows(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("gate-2").build()) {
            engine.start();
            assertThrows(IllegalStateException.class, engine::start,
                "Second start() must throw");
        }
    }

    @Test
    void isRunningFalseBeforeStart(@TempDir Path tmp) throws Exception {
        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("gate-3").build()) {
            assertFalse(engine.isRunning(),
                "running must be false before start()");
            engine.start();
            assertTrue(engine.isRunning(),
                "running must be true after start()");
        }
    }

    @Test
    void closeOnNeverStartedReleasesResources(@TempDir Path tmp) {
        LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("gate-4").build();
        assertDoesNotThrow(() -> engine.close(),
            "close() on never-started engine must not throw");
        // close() must have actually executed the cleanup path (not just returned early).
        // After close(), start() must be rejected -- proving the closed flag was set
        // and resources were released (Arena closed, cannot restart).
        assertThrows(IllegalStateException.class, engine::start,
            "start() after close() on never-started engine must throw");
    }

    @Test
    void startAfterCloseThrows(@TempDir Path tmp) throws Exception {
        LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).nodeId("gate-5").build();
        engine.start();
        engine.close();
        IllegalStateException ex = assertThrows(IllegalStateException.class, engine::start,
            "start() after close() must throw");
        assertTrue(ex.getMessage().contains("closed"),
            "Exception message must mention closed state");
    }
}
