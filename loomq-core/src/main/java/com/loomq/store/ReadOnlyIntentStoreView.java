package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.util.List;
import java.util.Map;

/**
 * IntentStore 的只读视图。
 *
 * 代理所有读操作，写操作抛出 {@link UnsupportedOperationException}。
 * 用于 {@link com.loomq.LoomqEngine#getIntentStore()} 返回给外部调用方，
 * 防止绕过 IntentCommandService 和 WAL 直接修改存储。
 */
public final class ReadOnlyIntentStoreView implements IntentStore {

    private final IntentStore delegate;

    public ReadOnlyIntentStoreView(IntentStore delegate) {
        this.delegate = delegate;
    }

    // ========== 读操作：代理到 delegate ==========

    @Override
    public Intent findById(String intentId) {
        return delegate.findById(intentId);
    }

    @Override
    public Map<String, Intent> getAllIntents() {
        return delegate.getAllIntents();
    }

    @Override
    public long count() {
        return delegate.count();
    }

    @Override
    public long countByStatus(IntentStatus status) {
        return delegate.countByStatus(status);
    }

    @Override
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        return delegate.checkIdempotency(idempotencyKey);
    }

    @Override
    public long getPendingCount() {
        return delegate.getPendingCount();
    }

    @Override
    public List<Intent> findByStatus(IntentStatus status, int offset, int limit) {
        return delegate.findByStatus(status, offset, limit);
    }

    // ========== 写操作：拒绝 ==========

    @Override
    public void save(Intent intent) {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }

    @Override
    public void update(Intent intent) {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }

    @Override
    public void delete(String intentId) {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }

    @Override
    public void upsert(Intent intent) {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Read-only IntentStore view");
    }
}
