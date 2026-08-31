package com.loomq.application.scheduler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.domain.intent.WalMode;
import com.loomq.spi.DeliveryHandler;
import com.loomq.spi.DeliveryHandler.DeliveryResult;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IntentStore;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * R21: Intent.setTags 存原始引用——用户持有同一 Map 在投递前并发变异（写入 null 值）时,
 * 消费者快照 intent.copy() 经构造器 Map.copyOf(tags) 抛 NPE。消费者 VT 是固定 Thread[]
 * 无监督设计,循环体无外层 try/catch:单发档 consumerCount=1 时该档投递永久停摆,
 * 已出队 intent 静默丢失(store 保持 SCHEDULED 直到重启),已 acquire 的 permit 永不释放。
 *
 * <p>修复:setTags 防御性拷贝(与构造器同款 Map.copyOf),内核不再持有用户可变引用——
 * 快照路径对用户后续变异免疫,非法输入(null key/value)在 API 边界即抛 NPE 而非杀消费者。</p>
 */
class BugSharedTagsMapKillsConsumerTest {

    private PrecisionScheduler scheduler;

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
    }

    /** 单消费者 ULTRA 目录:消费者死亡 = 档位投递停摆,可确定性断言。 */
    private static PrecisionTierCatalog singleConsumerCatalog() {
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        profiles.put(PrecisionTier.ULTRA, new PrecisionTierProfile(10, 200, 1, 5, 1, 200 * 16,
            WalMode.DURABLE, 10, false, true, 200_000));
        profiles.put(PrecisionTier.FAST, new PrecisionTierProfile(50, 150, 1, 10, 1, 150 * 16,
            WalMode.DURABLE, 50, false, true, 200_000));
        profiles.put(PrecisionTier.STANDARD, new PrecisionTierProfile(500, 50, 20, 100, 1, 50 * 16,
            WalMode.DURABLE, 500, false, true, 200_000));
        profiles.put(PrecisionTier.MILLI, new PrecisionTierProfile(1, 100, 1, 1, 1, 100 * 16,
            WalMode.DURABLE, 1, true, true, 200_000));
        return PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);
    }

    @Test
    void consumerMustSurviveUserTagsMapMutationAfterSetTags() throws Exception {
        IntentStore store = new ConcurrentIntentStore();
        CountDownLatch delivered = new CountDownLatch(1);
        DeliveryHandler handler = intent -> {
            delivered.countDown();
            return CompletableFuture.completedFuture(DeliveryResult.SUCCESS);
        };
        scheduler = new PrecisionScheduler(store, handler, null, singleConsumerCatalog());
        scheduler.start();

        Intent intent = new Intent("r21-tags-0001");
        intent.setExecuteAt(Instant.now().minusMillis(5));
        intent.transitionTo(IntentStatus.SCHEDULED);
        // 用户持有同一 Map:setTags 后继续写入 null 值(API 未禁止;修复前内核存原始引用,
        // 消费者快照 Map.copyOf 抛 NPE → 消费者 VT 死亡 → 永不投递)
        Map<String, String> userTags = new HashMap<>();
        userTags.put("k", "v");
        intent.setTags(userTags);
        userTags.put("bad", null); // 投递前用户侧变异
        store.save(intent);
        scheduler.schedule(intent);

        assertTrue(delivered.await(3, TimeUnit.SECONDS),
            "delivery must survive user mutation of the tags map after setTags (consumer killed by copy NPE)");
        // 防御性拷贝后,内核持有的 tags 是 setTags 时刻的快照,不含后续 null 变异
        assertFalse(intent.getTags().containsKey("bad"),
            "kernel must not see post-setTags mutations of the user's map");
    }
}
