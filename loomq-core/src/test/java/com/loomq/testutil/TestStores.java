package com.loomq.testutil;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.store.ConcurrentIntentStore;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.util.Map;

/**
 * 测试共享 IntentStore 实现。
 *
 * <p>收敛多个测试文件重复的“update() 抛错”注入 store，用于验证提交后失败/
 * 调度器结算容错路径。</p>
 */
public final class TestStores {

    private TestStores() {
    }

    /** 委托 ConcurrentIntentStore，仅 update() 抛错。 */
    public static final class UpdateThrowingStore implements IntentStore {
        private final ConcurrentIntentStore delegate = new ConcurrentIntentStore();

        @Override public void save(Intent intent) { delegate.save(intent); }
        @Override public void update(Intent intent) {
            throw new IllegalStateException("injected store failure after durable commit");
        }
        @Override public Intent findById(String intentId) { return delegate.findById(intentId); }
        @Override public Intent findByIdInternal(String intentId) { return delegate.findByIdInternal(intentId); }
        @Override public void delete(String intentId) { delegate.delete(intentId); }
        @Override public Map<String, Intent> getAllIntents() { return delegate.getAllIntents(); }
        @Override public long countByStatus(IntentStatus status) { return delegate.countByStatus(status); }
        @Override public IdempotencyResult checkIdempotency(String idempotencyKey) { return delegate.checkIdempotency(idempotencyKey); }
        @Override public long getPendingCount() { return delegate.getPendingCount(); }
        @Override public void shutdown() { delegate.shutdown(); }
    }
}
