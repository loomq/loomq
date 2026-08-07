package com.loomq.store;

import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.IntentStatus;
import java.util.List;
import java.util.Map;

/**
 * Intent 存储接口。
 *
 * 内核通过此接口访问 Intent 存储，实现可替换（内存、RocksDB 等）。
 * 默认实现为 ConcurrentIntentStore（基于 ConcurrentHashMap）。
 *
 * @author loomq
 * @since v0.9.0
 */
public interface IntentStore extends AutoCloseable {

    /** 保存新 Intent */
    void save(Intent intent);

    /** 更新已存在的 Intent */
    void update(Intent intent);

    /** 按 ID 查找 Intent（返回防御性拷贝，外部只读路径使用） */
    Intent findById(String intentId);

    /**
     * 按 ID 查找 Intent（返回 store 内部引用，命令服务内部写路径使用）。
     *
     * <p>与 {@link #findById(String)} 不同，此方法返回 store 内部持有的可变引用，
     * 调用方必须在 synchronized(intent) 块内操作，并确保后续写 WAL + update。
     * 仅限 IntentCommandService 等内部组件使用，不应暴露给外部调用方。</p>
     */
    default Intent findByIdInternal(String intentId) {
        return findById(intentId);
    }

    /** 删除 Intent */
    void delete(String intentId);

    /**
     * 写入或更新 Intent。
     *
     * 适用于”应用当前态”的场景，例如快照恢复、WAL 回放。
     */
    default void upsert(Intent intent) {
        Intent existing = findById(intent.getIntentId());
        if (existing == null) {
            save(intent);
        } else {
            update(intent);
        }
    }

    /** 清空整个 Intent 存储（快照恢复/重置用） */
    default void clear() {
        getAllIntents().keySet().forEach(this::delete);
    }

    /** 获取所有 Intent（恢复/快照用，大数据量场景慎用） */
    Map<String, Intent> getAllIntents();

    /**
     * 获取存储中的 intent 总数。
     *
     * @return intent 总数
     */
    default long count() {
        return getAllIntents().size();
    }

    /** 按状态统计数量 */
    long countByStatus(IntentStatus status);

    /** 幂等性检查 */
    IdempotencyResult checkIdempotency(String idempotencyKey);

    /** 待处理 Intent 数量 */
    long getPendingCount();

    /**
     * 按状态查询 Intent，支持分页。
     *
     * @param status 目标状态
     * @param offset 跳过前 N 条
     * @param limit  最大返回条数
     * @return 匹配的 Intent 列表
     */
    default List<Intent> findByStatus(IntentStatus status, int offset, int limit) {
        return getAllIntents().values().stream()
            .filter(i -> i.getStatus() == status)
            .skip(offset)
            .limit(limit)
            .toList();
    }

    /** 关闭存储，释放资源 */
    void shutdown();

    /** AutoCloseable 桥接，委托给 shutdown() */
    @Override
    default void close() {
        shutdown();
    }
}
