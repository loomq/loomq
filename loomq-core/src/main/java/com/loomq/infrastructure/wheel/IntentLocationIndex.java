package com.loomq.infrastructure.wheel;

import java.util.concurrent.ConcurrentHashMap;

/**
 * intentId → SlotLocation 索引。内存态(槽即当前态,可由扫描重建)。
 * 支持 cancel/firedNow 定位冷(>60min,不在 IntentStore)Intent 的磁盘槽。
 *
 * <p>实现 {@link AutoCloseable} 仅为契合 try-with-resources 用法(与 WheelStore/TailIndex
 * 风格一致);本类不持有任何 OS 资源,{@link #close()} 为 no-op。需显式清空索引时调用
 * {@link #clear()}。</p>
 */
public final class IntentLocationIndex implements AutoCloseable {
    private final ConcurrentHashMap<String, SlotLocation> map = new ConcurrentHashMap<>();

    public void put(String intentId, SlotLocation loc) { map.put(intentId, loc); }
    public SlotLocation get(String intentId) { return map.get(intentId); }
    public boolean remove(String intentId) { return map.remove(intentId) != null; }
    public void clear() { map.clear(); }
    public int size() { return map.size(); }

    @Override
    public void close() {
        // no-op:纯内存索引,无 OS 资源需释放。保留 try-with-resources 语法一致性。
    }
}
