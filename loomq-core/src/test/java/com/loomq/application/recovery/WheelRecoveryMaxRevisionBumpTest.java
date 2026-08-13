package com.loomq.application.recovery;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.application.scheduler.PrecisionScheduler;
import com.loomq.common.MetricsCollector;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.infrastructure.wheel.IntentLocationIndex;
import com.loomq.infrastructure.wheel.PromotionDaemon;
import com.loomq.infrastructure.wheel.TailIndex;
import com.loomq.infrastructure.wheel.WheelConfig;
import com.loomq.infrastructure.wheel.WheelStore;
import com.loomq.spi.DeliveryHandler;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.testutil.TestWheelConfigs;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R10: 恢复期把停机窗口到期 Intent 原地终态化时会 incrementRevision(+1),但
 * {@link WheelRecovery} 的 maxRevisions 报告在该 +1 之前构建——报告里记录的是旧 revision。
 * createIntent 重建同 intentId 时据此种子 revision,新槽 revision 与终态墓碑 revision 打平;
 * recovery 按 max revision 去重用严格 &gt;(平票先扫到者胜),过去时刻的终态槽(更小 bucket key
 * 先被扫到)会遮蔽重建的新 SCHEDULED 槽 → 静默丢投递。maxRevisions 必须反映终态化后的 revision。
 */
class WheelRecoveryMaxRevisionBumpTest {

    @TempDir Path tmp;

    @Test
    void overdueTerminalizationMustBumpReportedMaxRevision() {
        AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        WheelConfig cfg = TestWheelConfigs.defaults(tmp);
        String id = "r10-overdue-0001";

        // 停机窗口到期的 SCHEDULED 单槽,rev=1
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            Intent overdue = new Intent(id);
            overdue.setExecuteAt(Instant.ofEpochMilli(clock.get() - 60_000));
            overdue.transitionTo(IntentStatus.SCHEDULED);
            overdue.incrementRevision();   // rev = 1
            store.put(overdue);
        }

        // 恢复:到期 → 原地终态化(rev 1 → 2)。maxRevisions 报告必须为 2 而非 1。
        try (WheelStore store2 = new WheelStore(cfg, clock::get);
             TailIndex tail2 = new TailIndex(tmp, clock::get);
             ConcurrentIntentStore mem = new ConcurrentIntentStore();
             IntentLocationIndex idx = new IntentLocationIndex();
             PromotionDaemon daemon = new PromotionDaemon(store2, tail2, idx, clock::get, (i, loc) -> {}, 60_000L)) {
            PrecisionScheduler scheduler = new PrecisionScheduler(
                mem, i -> CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS), null);
            WheelRecoveryReport rpt = new WheelRecovery(store2, tail2, 60L * 60_000L, new MetricsCollector())
                .recover(mem, scheduler, idx, daemon);

            assertEquals(2L, rpt.maxRevisions().get(id),
                "overdue terminalization bumps rev 1->2; reported max revision must be 2, "
                    + "otherwise a recreate would tie with the terminal slot and be ghost-shadowed");
        }
    }
}
