package com.loomq.testutil;

import static org.junit.jupiter.api.Assertions.fail;

import com.loomq.LoomqEngine;
import com.loomq.domain.intent.Intent;
import java.time.Duration;
import java.util.Optional;

/**
 * 测试共享夹具。引擎构造样板/SUCCESS handler 等后续可并入;先收敛逐字重复的
 * awaitTerminal 轮询(techdebt D3:5 个测试文件各自实现,超时/轮询参数漂移)。
 */
public final class TestEngines {

    private TestEngines() {
    }

    /** 轮询等待 intent 进入终态,超时 fail。 */
    public static void awaitTerminal(LoomqEngine engine, String intentId, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            Optional<Intent> cur = engine.getIntent(intentId);
            if (cur.isPresent() && cur.get().getStatus().isTerminal()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("intent " + intentId + " did not reach terminal state within " + timeout);
    }
}
