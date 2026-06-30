package com.loomq.storage;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import com.loomq.store.IdempotencyResult;
import com.loomq.store.IntentStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 装饰器：将 IntentStore 方法调用 offload 到平台线程池。
 *
 * 解决 RocksDB JNI 调用在虚拟线程上执行时 pin carrier 线程的问题。
 * 平台线程执行 JNI 时，虚拟线程在 {@code join()} 上 park（而非 pin），
 * 不会占用 ForkJoinPool 的 carrier 线程。
 *
 * <p>用法：
 * <pre>{@code
 * IntentStore rawStore = new RocksDBIntentStore(config);
 * IntentStore store = new PlatformThreadIntentStore(rawStore);
 * engineBuilder.intentStore(store);
 * }</pre>
 *
 * @author loomq
 * @since v0.9.2
 */
public final class PlatformThreadIntentStore implements IntentStore {

    private static final Logger logger = LoggerFactory.getLogger(PlatformThreadIntentStore.class);

    private final IntentStore delegate;
    private final ExecutorService platformExecutor;

    /**
     * @param delegate    被包装的实际存储实现
     * @param threadCount 平台线程池大小
     */
    public PlatformThreadIntentStore(IntentStore delegate, int threadCount) {
        this.delegate = delegate;
        this.platformExecutor = Executors.newFixedThreadPool(threadCount,
            Thread.ofPlatform().name("store-io-", 0).factory());
        logger.info("PlatformThreadIntentStore initialized: threads={}", threadCount);
    }

    /**
     * 使用默认线程数（CPU 核心数）的便捷构造器。
     */
    public PlatformThreadIntentStore(IntentStore delegate) {
        this(delegate, Runtime.getRuntime().availableProcessors());
    }

    /**
     * 将操作 offload 到平台线程并同步等待结果。
     */
    private <T> T offload(Callable<T> action) {
        try {
            return platformExecutor.submit(action).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Store operation interrupted", e);
        }
    }

    /**
     * 将无返回值操作 offload 到平台线程并同步等待完成。
     */
    private void offloadVoid(Runnable action) {
        offload(() -> {
            action.run();
            return null;
        });
    }

    // ========== IntentStore 接口实现 ==========

    @Override
    public void save(Intent intent) {
        offloadVoid(() -> delegate.save(intent));
    }

    @Override
    public void update(Intent intent) {
        offloadVoid(() -> delegate.update(intent));
    }

    @Override
    public Intent findById(String intentId) {
        return offload(() -> delegate.findById(intentId));
    }

    @Override
    public Intent findByIdInternal(String intentId) {
        return offload(() -> delegate.findByIdInternal(intentId));
    }

    @Override
    public void delete(String intentId) {
        offloadVoid(() -> delegate.delete(intentId));
    }

    @Override
    public void upsert(Intent intent) {
        offloadVoid(() -> delegate.upsert(intent));
    }

    @Override
    public void clear() {
        offloadVoid(delegate::clear);
    }

    @Override
    public Map<String, Intent> getAllIntents() {
        return offload(delegate::getAllIntents);
    }

    @Override
    public long count() {
        return offload(delegate::count);
    }

    @Override
    public long countByStatus(IntentStatus status) {
        return offload(() -> delegate.countByStatus(status));
    }

    @Override
    public IdempotencyResult checkIdempotency(String idempotencyKey) {
        return offload(() -> delegate.checkIdempotency(idempotencyKey));
    }

    @Override
    public long getPendingCount() {
        return offload(delegate::getPendingCount);
    }

    @Override
    public List<Intent> findByStatus(IntentStatus status, int offset, int limit) {
        return offload(() -> delegate.findByStatus(status, offset, limit));
    }

    @Override
    public void shutdown() {
        platformExecutor.shutdown();
        try {
            if (!platformExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                platformExecutor.shutdownNow();
                logger.warn("PlatformThreadIntentStore: executor did not terminate in 5s, forced shutdown");
            }
        } catch (InterruptedException e) {
            platformExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        delegate.shutdown();
    }
}
