package com.loomq.infrastructure.wheel;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * intentId → SlotLocation 索引。内存态(槽即当前态,可由扫描重建)。
 * 支持 cancel/firedNow 定位冷(>60min,不在 IntentStore)Intent 的磁盘槽。
 *
 * <p>实现 {@link AutoCloseable} 仅为契合 try-with-resources 用法(与 WheelStore/TailIndex
 * 风格一致);本类不持有任何 OS 资源,{@link #close()} 为 no-op。</p>
 */
public final class IntentLocationIndex implements AutoCloseable {
    private final ConcurrentHashMap<String, SlotLocation> map = new ConcurrentHashMap<>();

    public void put(String intentId, SlotLocation loc) { map.put(intentId, loc); }
    public SlotLocation get(String intentId) { return map.get(intentId); }
    public boolean remove(String intentId) { return map.remove(intentId) != null; }

    /**
     * 定向移除:仅当索引仍指向 {@code expected} 时才移除(C2-1)。
     * 终态回收按"自己终态化的槽"清理——并发重建(磁盘终态允许同 id 重建)已把索引指向
     * 新槽时不得抹除,否则冷 intent 到点不被 promote,静默不投递直到重启。
     */
    public boolean remove(String intentId, SlotLocation expected) {
        return map.remove(intentId, expected);
    }

    /** 返回所有索引中的 SlotLocation(供 BucketReclaimer 遍历判断活跃桶)。 */
    public Collection<SlotLocation> allLocations() { return map.values(); }

    @Override
    public void close() {
        // no-op:纯内存索引,无 OS 资源需释放。保留 try-with-resources 语法一致性。
    }
}
