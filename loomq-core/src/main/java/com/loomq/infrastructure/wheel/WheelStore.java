package com.loomq.infrastructure.wheel;

import com.loomq.domain.intent.Intent;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 持久化分层时间轮(四轮)。每桶 = 一个 mmap 文件,含 slotsPerBucket 个定长槽。
 * 桶按 bucketKey=floor(executeAt/window) 寻址,绝对(非滚动),旧桶可删。
 * 超出 day 视界 → tail(本类只标记 inTail,实际存储由 TailIndex 负责)。
 */
public final class WheelStore implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WheelStore.class);

    private final WheelConfig config;
    private final LongSupplier clock;
    private final int slotsPerBucket;
    private final int bucketBytes;
    private final long horizonMs;

    // tier → (bucketKey → Bucket)
    private final Map<WheelTier, ConcurrentHashMap<Long, Bucket>> wheels = new EnumMap<>(WheelTier.class);
    private final Arena arena = Arena.ofShared();
    /** 溢出链 spill 计数：按源档统计桶满后溢出到下一档的次数。 */
    private final EnumMap<WheelTier, AtomicLong> spillCounts = new EnumMap<>(WheelTier.class);
    /** 空槽模板:status=0。free 时整槽清零,readSlot/isOccupied 据此判空。 */
    private static final byte[] EMPTY_SLOT = new byte[SlotCodec.SLOT_SIZE];

    /** Test-only: deleteBucket 在关闭旧桶之后、删除文件之前触发(TOCTOU 重建桶测试用)。 */
    volatile Runnable testBeforeFileDeleteHook;

    public WheelStore(WheelConfig config, LongSupplier clock) {
        this.config = config;
        this.clock = clock;
        this.slotsPerBucket = config.slotsPerBucket();
        this.bucketBytes = slotsPerBucket * SlotCodec.SLOT_SIZE;
        this.horizonMs = (long) config.horizonDays() * WheelTier.DAY.windowMs; // 由 config.horizonDays() 覆盖
        for (WheelTier t : WheelTier.values()) {
            wheels.put(t, new ConcurrentHashMap<>());
            spillCounts.put(t, new AtomicLong());
        }
        loadExistingBuckets();
    }

    /** 启动时从磁盘载入已存在的桶(镜像 SimpleWalWriter.loadExistingSegments)。 */
    private void loadExistingBuckets() {
        for (WheelTier tier : WheelTier.values()) {
            Path dir = Paths.get(config.dataDir(), tier.name().toLowerCase());
            if (!Files.isDirectory(dir)) continue;
            try (var stream = Files.list(dir)) {
                stream.filter(p -> p.getFileName().toString().endsWith(".bin"))
                     .forEach(p -> {
                         try {
                             long key = Long.parseLong(p.getFileName().toString().replace(".bin", ""));
                             Bucket b = openBucket(tier, key);
                             b.rebuildFreeList();
                             wheels.get(tier).put(key, b);
                         } catch (Exception e) { log.warn("skip bucket {}", p, e); }
                     });
            } catch (IOException e) { log.warn("load buckets for {} failed", tier, e); }
        }
    }

    public SlotLocation locate(Instant executeAt) {
        long now = clock.getAsLong();
        long delta = executeAt.toEpochMilli() - now;
        if (delta > horizonMs) return SlotLocation.tail(executeAt.toEpochMilli());
        WheelTier tier = pickTier(delta);
        long bucketKey = executeAt.toEpochMilli() / tier.windowMs;
        return new SlotLocation(tier, bucketKey, -1, false);
    }

    /**
     * 写入 intent 并返回实际分配的槽位。桶满时经溢出链落到下一层更粗档
     * （SEC→MIN→HOUR→DAY），突破单桶容量硬顶；仅当整条链都满（DAY 满）才抛
     * {@link SlotOverflowException}。溢出链 try 仅包 {@code alloc()}：payload 超 210B 的
     * {@link SlotCodec#encode} 溢出（同为 SlotOverflowException）不被误判为桶满而 spill；
     * encode 失败会将刚 alloc 的保留槽回滚（releaseReserved），避免烧槽致后续同秒 put 误 spill 降级。
     * 桶被回收（{@link BucketClosedException}）时移除死桶引用并重试进新桶（有界 3 次），
     * 绝不放行写入已关闭桶——否则 create 返回成功但 Intent 字节随删桶文件静默消失。
     */
    public SlotLocation put(Intent intent) {
        SlotLocation loc = locate(intent.getExecuteAt());
        if (loc.inTail()) {
            throw new IllegalStateException("out-of-horizon put must go via TailIndex; got " + loc);
        }
        WheelTier tier = loc.tier();
        long executeAtMs = intent.getExecuteAt().toEpochMilli();
        int closedRetries = 0;
        while (true) {
            long bucketKey = executeAtMs / tier.windowMs;
            Bucket b = getOrCreateBucket(tier, bucketKey);
            int slot;
            try {
                slot = b.alloc();
            } catch (SlotOverflowException e) {
                WheelTier coarser = tier.nextCoarser();
                if (coarser == null) throw e;              // 链终点(DAY)仍满 → 原样抛出
                spillCounts.get(tier).incrementAndGet();
                log.warn("Bucket overflow {} -> spill {} to {}", tier, intent.getIntentId(), coarser);
                tier = coarser;
                continue;
            }
            byte[] encoded;
            try {
                encoded = SlotCodec.encode(intent);
            } catch (RuntimeException e) {
                b.releaseReserved(slot);   // 编码失败回滚保留槽，避免烧槽致后续同秒 put 误 spill
                throw e;
            }
            try {
                b.write(slot, encoded);
            } catch (BucketClosedException e) {
                // deleteBucket 与在途 put 竞态:put 持有旧桶引用,桶已被回收(close)。
                // 移除死桶引用、重试进新桶(有界);连续失败显式抛错——可见失败,不静默丢失。
                wheels.get(tier).remove(bucketKey, b);
                if (++closedRetries >= 3) {
                    throw new IllegalStateException(
                        "bucket closed repeatedly on put: " + tier + "/" + bucketKey, e);
                }
                continue;
            }
            return new SlotLocation(tier, bucketKey, slot, false);
        }
    }

    public Intent readSlot(SlotLocation loc) {
        if (loc.inTail()) return null;
        if (loc.slotIndex() < 0) return null;
        Bucket b = wheels.get(loc.tier()).get(loc.bucketKey());
        if (b == null) return null;
        byte[] slot = b.read(loc.slotIndex());
        if (!SlotCodec.isOccupied(slot) || SlotCodec.isTorn(slot)) return null;
        return SlotCodec.decode(slot);
    }

    /**
     * 流式扫描所有有效槽(from 起始),返回惰性迭代器。
     * 不预物化到 ArrayList--按 tier -> bucket -> slot 顺序逐个读取 MemorySegment。
     * 空槽和 torn 槽被跳过。
     */
    public java.util.Iterator<SlotEntry> scanSlotsFrom(java.time.Instant from) {
        return new LazySlotIterator(from.toEpochMilli());
    }

    /**
     * 惰性槽迭代器:按 tier -> bucket(sorted keys) -> slot(0..N) 顺序读取。
     * hasNext() 时扫描到下一个有效槽并缓存;next() 返回缓存并推进。
     * 跳过时间窗口终点 <= fromMs 的桶(已无需恢复的过期桶)。
     */
    private final class LazySlotIterator implements java.util.Iterator<SlotEntry> {
        private final java.util.List<WheelTier> tiers = java.util.List.of(WheelTier.values());
        private final long fromMs;
        private int tierIdx = 0;
        private java.util.List<Long> bucketKeys;
        private int bucketIdx = 0;
        private Bucket currentBucket;
        private int slotIdx = 0;
        private SlotEntry cached;

        LazySlotIterator(long fromMs) {
            this.fromMs = fromMs;
            advanceTier();
        }

        private void advanceTier() {
            while (tierIdx < tiers.size()) {
                WheelTier tier = tiers.get(tierIdx);
                ConcurrentHashMap<Long, Bucket> buckets = wheels.get(tier);
                bucketKeys = new java.util.ArrayList<>();
                for (long key : buckets.keySet()) {
                    if (key * tier.windowMs + tier.windowMs <= fromMs) continue;
                    bucketKeys.add(key);
                }
                java.util.Collections.sort(bucketKeys);
                bucketIdx = 0;
                if (!bucketKeys.isEmpty()) {
                    currentBucket = buckets.get(bucketKeys.get(0));
                    slotIdx = 0;
                    return;
                }
                tierIdx++;
            }
            currentBucket = null;
        }

        private void advanceBucket() {
            bucketIdx++;
            if (bucketIdx < bucketKeys.size()) {
                WheelTier tier = tiers.get(tierIdx);
                currentBucket = wheels.get(tier).get(bucketKeys.get(bucketIdx));
                slotIdx = 0;
            } else {
                tierIdx++;
                advanceTier();
            }
        }

        @Override public boolean hasNext() {
            if (cached != null) return true;
            while (currentBucket != null) {
                while (slotIdx < slotsPerBucket) {
                    byte[] slot = currentBucket.read(slotIdx);
                    slotIdx++;
                    if (SlotCodec.isOccupied(slot) && !SlotCodec.isTorn(slot)) {
                        Intent it = SlotCodec.decode(slot);
                        WheelTier tier = tiers.get(tierIdx);
                        long key = bucketKeys.get(bucketIdx);
                        SlotLocation loc = new SlotLocation(tier, key, slotIdx - 1, false);
                        cached = new SlotEntry(loc, it);
                        return true;
                    }
                }
                advanceBucket();
            }
            return false;
        }

        @Override public SlotEntry next() {
            if (cached == null && !hasNext()) throw new java.util.NoSuchElementException();
            SlotEntry result = cached;
            cached = null;
            return result;
        }
    }

    public void forceDirty() {
        for (var buckets : wheels.values()) {
            for (Bucket b : buckets.values()) {
                b.forceIfDirty();
            }
        }
    }

    public long getMinTailExecuteAt() { return clock.getAsLong() + horizonMs; }

    /**
     * 最新桶文件的最后写入时刻(mtime,毫秒):恢复期时钟回拨守卫的磁盘证据。
     * 引擎写入/force 会推进桶文件 mtime;若重启时钟早于该证据(停机期间回拨),
     * 恢复的 overdue 判定不可信。无桶时返回 0(无证据,守卫不生效)。
     */
    public long newestBucketWriteTimeMs() {
        long max = 0;
        for (var buckets : wheels.values()) {
            for (Bucket b : buckets.values()) {
                try {
                    long m = Files.getLastModifiedTime(b.path).toMillis();
                    if (m > max) max = m;
                } catch (IOException ignored) {
                    // 取证性守卫:个别文件读不到 mtime 不影响其余证据
                }
            }
        }
        return max;
    }

    /** 列出指定 tier 的所有桶 key(供 BucketReclaimer 遍历)。 */
    public Set<Long> listBucketKeys(WheelTier tier) {
        return wheels.get(tier).keySet();
    }

    /**
     * 删除指定桶:close(force 脏数据)-> 从 wheels 移除 -> 删除文件。
     * 仅在桶无活跃引用(BucketReclaimer 已确认)时调用。
     */
    public void deleteBucket(WheelTier tier, long bucketKey) {
        Bucket b = wheels.get(tier).remove(bucketKey);
        if (b == null) return;
        b.close();
        if (testBeforeFileDeleteHook != null) {
            testBeforeFileDeleteHook.run();
        }
        // TOCTOU 守卫:close 之后、删文件之前,同 key 桶可能已被并发 put() 重建
        // (computeIfAbsent 安装新桶、重新打开文件,BucketReclaimer 的引用快照是 stale 的)。
        // 此时删文件会命中新桶的落盘文件——新 Intent 的槽字节随文件消失,重启静默丢失。
        // 仅当 map 中该 key 已无其他桶对象时才删除文件。
        if (wheels.get(tier).get(bucketKey) != null) {
            log.warn("Skip deleting bucket file {}/{}: recreated concurrently after map removal",
                tier, bucketKey);
            return;
        }
        try {
            Files.deleteIfExists(Paths.get(config.dataDir(), tier.name().toLowerCase(),
                String.format("%020d.bin", bucketKey)));
            log.info("Deleted expired bucket: {}/{}", tier, bucketKey);
        } catch (IOException e) {
            log.warn("Failed to delete bucket file: {}/{}", tier, bucketKey, e);
        }
    }

    /** 原地覆写已有槽（终态原地写，不分配新槽）。tail / 缺桶 / 缺槽则 no-op。 */
    public void overwriteSlot(SlotLocation loc, byte[] encoded) {
        if (loc.inTail() || loc.slotIndex() < 0) return;
        Bucket b = wheels.get(loc.tier()).get(loc.bucketKey());
        if (b == null) return;
        if (!SlotCodec.isOccupied(b.read(loc.slotIndex()))) return;  // 空槽(已回收未复用)不覆写(防复活);已复用槽会被覆写——调用方须以 locationIndex 为准,回收前先清索引
        b.write(loc.slotIndex(), encoded);
    }

    /** 回收槽：清空 + 入 free-list。tail / 缺桶 / 缺槽则 no-op。 */
    public void freeSlot(SlotLocation loc) {
        if (loc.inTail() || loc.slotIndex() < 0) return;
        Bucket b = wheels.get(loc.tier()).get(loc.bucketKey());
        if (b == null) return;
        b.free(loc.slotIndex());
    }

    /** 溢出链 spill 计数快照（按源档；未溢出档为 0）。 */
    public Map<WheelTier, Long> getSpillCounts() {
        Map<WheelTier, Long> out = new EnumMap<>(WheelTier.class);
        spillCounts.forEach((t, c) -> out.put(t, c.get()));
        return out;
    }

    /** 暴露 clock(供 BucketReclaimer 判断过期)。 */
    public LongSupplier clock() { return clock; }

    private WheelTier pickTier(long delta) {
        if (delta <= WheelTier.SEC.windowMs * WheelTier.SEC.count) return WheelTier.SEC;
        if (delta <= WheelTier.MIN.windowMs * WheelTier.MIN.count) return WheelTier.MIN;
        if (delta <= WheelTier.HOUR.windowMs * WheelTier.HOUR.count) return WheelTier.HOUR;
        return WheelTier.DAY;
    }

    private Bucket getOrCreateBucket(WheelTier tier, long bucketKey) {
        return wheels.get(tier).computeIfAbsent(bucketKey, k -> {
            try { return openBucket(tier, k); }
            catch (IOException e) { throw new RuntimeException("open bucket failed: " + tier + "/" + k, e); }
        });
    }

    private Bucket openBucket(WheelTier tier, long bucketKey) throws IOException {
        Path dir = Paths.get(config.dataDir(), tier.name().toLowerCase());
        Files.createDirectories(dir);
        Path file = dir.resolve(String.format("%020d.bin", bucketKey));
        FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (ch.size() < bucketBytes) {
            // FileChannel.truncate 只缩不扩:新建 0 字节文件 truncate 后仍为 0,mmap 映射超 EOF
            // 的页在 Linux 上首次访问即 SIGBUS 进程崩溃(Windows CreateFileMapping 自动扩展只是侥幸)。
            // 在末字节位置写 1 字节,把文件显式扩展到 bucketBytes 后再映射。
            ch.write(ByteBuffer.allocate(1), bucketBytes - 1L);
        }
        MemorySegment mapped = ch.map(FileChannel.MapMode.READ_WRITE, 0, bucketBytes, arena);
        return new Bucket(tier, bucketKey, file, ch, mapped);
    }

    @Override public void close() {
        forceDirty();
        for (var buckets : wheels.values()) for (Bucket b : buckets.values()) b.close();
        arena.close();
    }

    /** 单桶:一个 mmap 文件。alloc 用 AtomicInteger 无锁分槽。 */
    private final class Bucket {
        final WheelTier tier; final long bucketKey; final Path path; final FileChannel channel;
        final MemorySegment seg; final AtomicInteger next = new AtomicInteger(0);
        /** 回收槽 free-list：alloc 先复用，空则 next 单调分配。纯内存可重建（重启全桶扫描）。 */
        final ConcurrentLinkedDeque<Integer> freeList = new ConcurrentLinkedDeque<>();
        private final AtomicLong writeCount = new AtomicLong();
        private volatile long flushedWriteCount = 0;
        private volatile boolean closed = false;
        /**
         * 写/force 互斥锁。P1-4: 写者持 readLock(彼此并发),force 持 writeLock(独占)。
         * 修复"memcpy 后未 increment 时 force 的 hasUnflushed 快照漏写 → frontier 误发布"
         * 的微窗口:写者在 readLock 内完成 memcpy+increment,force 在 writeLock 内
         * hasUnflushed 检查+seg.force+更新 flushedWriteCount,二者互斥,不再漏写。
         */
        private final ReadWriteLock forceLock = new ReentrantReadWriteLock();
        Bucket(WheelTier t, long k, Path p, FileChannel ch, MemorySegment m) { tier=t; bucketKey=k; path=p; channel=ch; seg=m; }
        int alloc() {
            Integer freed = freeList.poll();
            if (freed != null) return freed;
            int idx = next.getAndIncrement();
            if (idx >= slotsPerBucket) {
                // Wave3: 用可识别的 SlotOverflowException 替代裸 IllegalStateException,
                // put() 内桶满走溢出链 spill,此异常仅在整条链满(DAY)时向上抛。
                throw new SlotOverflowException("bucket overflow: " + tier + "/" + bucketKey
                    + " (slotsPerBucket=" + slotsPerBucket + "); spill chain exhausted at " + tier
                    + " — consider increasing slotsPerBucket");
            }
            return idx;
        }
        /** 释放刚 alloc 但尚未写入的保留槽（encode 失败回滚）；槽必为空，直接入 free-list。 */
        void releaseReserved(int slot) {
            freeList.push(slot);
        }
        /** 回收槽：写 status=0 空槽 + 入 free-list（供 alloc 复用）。双重 free 防护：已空槽不重复入栈。 */
        void free(int slot) {
            // C2-4: check-then-act 在 writeLock 内原子化——并发双 free 可同时通过占用检查、
            // 双双入栈(同槽两份 → alloc 两次发出同一槽,后写者覆写先写者)。当前调用方
            // (reclaimTerminal 单消费)不可达,防御未来并发回收路径。
            Lock wl = forceLock.writeLock();
            wl.lock();
            try {
                if (closed) return;
                if (!SlotCodec.isOccupied(read(slot))) return;  // 双重 free 防护：已空槽不重复入栈
                write(slot, EMPTY_SLOT);
                freeList.push(slot);
            } finally {
                wl.unlock();
            }
        }
        void write(int slot, byte[] data) {
            long off = (long) slot * SlotCodec.SLOT_SIZE;
            if (off + SlotCodec.SLOT_SIZE > seg.byteSize()) {  // 越界防护：与 read() 一致
                throw new IndexOutOfBoundsException("slot " + slot + " out of bounds for bucket " + tier + "/" + bucketKey);
            }
            Lock rl = forceLock.readLock();
            rl.lock();
            try {
                // deleteBucket 与在途 put 竞态:close() 在 writeLock 内置 closed+force,
                // 此处 readLock 互斥——close 已完成则必抛,绝不放行写入已关闭桶(静默丢失)。
                if (closed) throw new BucketClosedException("bucket closed: " + tier + "/" + bucketKey);
                MemorySegment src = MemorySegment.ofArray(data);
                MemorySegment.copy(src, 0, seg, off, data.length);
                writeCount.incrementAndGet();
            } finally {
                rl.unlock();
            }
        }
        byte[] read(int slot) {
            long off = (long) slot * SlotCodec.SLOT_SIZE;
            if (off + SlotCodec.SLOT_SIZE > seg.byteSize()) return new byte[SlotCodec.SLOT_SIZE];
            byte[] buf = new byte[SlotCodec.SLOT_SIZE];
            MemorySegment.copy(seg, off, MemorySegment.ofArray(buf), 0, SlotCodec.SLOT_SIZE);
            return buf;
        }
        /** 启动恢复：全桶扫描，空槽全部入 free-list；next 置 slotsPerBucket——freeList 已含全部空槽，
         *  next 不得 mint 其内部索引（否则 freeList 耗尽后 next 双重分配覆写）。alloc 先服 freeList，耗尽即真满抛。 */
        void rebuildFreeList() {
            freeList.clear();
            for (int i = 0; i < slotsPerBucket; i++) {
                if (!SlotCodec.isOccupied(read(i))) {
                    freeList.offer(i);
                }
            }
            next.set(slotsPerBucket);
        }
        boolean hasUnflushed() { return writeCount.get() > flushedWriteCount; }

        /** P1-4: 在 writeLock 内检查+force+更新 flushedWriteCount,与 write() 互斥。 */
        void forceIfDirty() {
            if (closed) return;
            Lock wl = forceLock.writeLock();
            wl.lock();
            try {
                if (!hasUnflushed()) return;
                long before = writeCount.get();
                seg.force();
                flushedWriteCount = before;
            } finally {
                wl.unlock();
            }
        }
        void close() {
            // writeLock 内置 closed+force:与 write() 的 readLock 互斥,消除
            // "write 检查 closed 通过 → close 执行 → write 落进已关闭段"的检查-拷贝窗口。
            Lock wl = forceLock.writeLock();
            wl.lock();
            try {
                if (closed) return;
                closed = true;
                try { seg.force(); } catch (Exception e) {
                    // P1-4: 不再吞异常——close 期 force 失败意味着脏数据可能未落盘,需可见。
                    log.warn("force on bucket close failed: {}/{}", tier, bucketKey, e);
                }
            } finally {
                wl.unlock();
            }
            try { channel.close(); } catch (IOException e) {
                log.warn("close channel failed: {}/{}", tier, bucketKey, e);
            }
        }
    }
}
