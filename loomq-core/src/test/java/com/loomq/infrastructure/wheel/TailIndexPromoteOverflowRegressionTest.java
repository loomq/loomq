package com.loomq.infrastructure.wheel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.loomq.domain.intent.Intent;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R6: TailIndex.promoteInto 对 DAY 桶满无防护——engine.start → WheelRecovery.recover 第一步
 * 即 promoteInto。同一自然日 >slotsPerBucket 条远期 Intent（都落在同一 DAY 桶，DAY 是
 * 溢出链终点）时 store.put 抛 SlotOverflowException：整条 promoteInto 中断且该 tail 条目
 * 不追加 tombstone、不移除 → 每次重启都在同一条目上失败，引擎被砖（运行期 createIntent
 * 同条件失败有补偿路径，恢复期无自愈）。修复：per-entry 捕获 SlotOverflowException，
 * 保留 tail 条目待下次恢复（DAY 桶随投递回收后）再试。
 */
class TailIndexPromoteOverflowRegressionTest {

    @TempDir Path tmp;

    @Test
    void promoteIntoMustNotBrickOnFullDayBucket() {
        AtomicLong clock = new AtomicLong(Instant.parse("2026-08-12T10:00:00Z").toEpochMilli());
        WheelConfig cfg = new WheelConfig(tmp.toString(), "t", 2, 2, 1, 10_000L,
            60L * 60_000L, 60_000L, null);
        try (WheelStore store = new WheelStore(cfg, clock::get);
             TailIndex tail = new TailIndex(tmp, clock::get)) {
            long t0 = clock.get();
            long x = t0 + 3L * 86_400_000L; // +3d：写入时在 tail（>2d horizon）
            tail.put(make("r6-promo-00001", x));

            // 推进时钟到 +1.1d（1584min）：x 进入 day 视界（delta=1.9d ≤ 2d horizon,>24h → DAY 档），
            // 两个同刻 intent 填满 day(x) 桶（slotsPerBucket=2）
            clock.set(t0 + 1_584L * 60_000L);
            store.put(make("r6-promo-00002", x));
            store.put(make("r6-promo-00003", x));

            // 推进到 +1.5d（2160min）：提升阈值（clock+2d）覆盖 x，tail 条目被提升 → DAY 桶满
            clock.set(t0 + 2_160L * 60_000L);
            int promoted = tail.promoteInto(store); // 修复前：SlotOverflowException 穿透

            assertEquals(0, promoted, "full DAY bucket must skip the entry, not throw");
            assertEquals(1, tail.size(), "tail entry must be kept for a later recovery");
        }
    }

    private static Intent make(String id, long executeAtMs) {
        Intent it = new Intent(id);
        it.setExecuteAt(Instant.ofEpochMilli(executeAtMs));
        return it;
    }
}
