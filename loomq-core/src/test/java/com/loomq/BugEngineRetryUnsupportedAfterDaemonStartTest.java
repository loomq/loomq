package com.loomq;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.loomq.spi.DeliveryHandler.DeliveryResult;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 如果上次 start() 在 daemon 启动后失败，再次 start() 应明确拒绝，并且 close() 仍可清理资源。
 */
class BugEngineRetryUnsupportedAfterDaemonStartTest {

    @TempDir Path tmp;

    @Test
    void startMustRejectRetryAfterDaemonStartFailureAndCloseMustStillWork() throws Exception {
        LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp.resolve("e")).nodeId("e1")
                .deliveryHandler(i -> CompletableFuture.completedFuture(DeliveryResult.SUCCESS))
                .build();

        Field daemonsField = LoomqEngine.class.getDeclaredField("daemonsStarted");
        daemonsField.setAccessible(true);
        daemonsField.setBoolean(engine, true);
        Field retryField = LoomqEngine.class.getDeclaredField("retryUnsupported");
        retryField.setAccessible(true);
        retryField.setBoolean(engine, true);

        assertThrows(IllegalStateException.class, engine::start,
            "start() must reject retry after daemons were already started in a failed attempt");
        engine.close();
    }
}
