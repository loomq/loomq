package com.loomq.infrastructure.wheel;

import com.loomq.domain.intent.Intent;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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

    public WheelStore(WheelConfig config, LongSupplier clock) {
        this.config = config;
        this.clock = clock;
        this.slotsPerBucket = config.slotsPerBucket();
        this.bucketBytes = slotsPerBucket * SlotCodec.SLOT_SIZE;
        this.horizonMs = (long) config.horizonDays() * WheelTier.DAY.windowMs; // 由 config.horizonDays() 覆盖
        for (WheelTier t : WheelTier.values()) wheels.put(t, new ConcurrentHashMap<>());
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
                             b.recoverHighWaterMark();
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

    public SlotLocation put(Intent intent) {
        SlotLocation loc = locate(intent.getExecuteAt());
        if (loc.inTail()) {
            throw new IllegalStateException("out-of-horizon put must go via TailIndex; got " + loc);
        }
        Bucket b = getOrCreateBucket(loc.tier(), loc.bucketKey());
        int slot = b.alloc();
        byte[] encoded = SlotCodec.encode(intent);
        b.write(slot, encoded);
        return new SlotLocation(loc.tier(), loc.bucketKey(), slot, false);
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

    public Iterator<Intent> scanFrom(Instant from) {
        long fromMs = from.toEpochMilli();
        List<Intent> acc = new ArrayList<>();
        for (WheelTier tier : WheelTier.values()) {
            ConcurrentHashMap<Long, Bucket> buckets = wheels.get(tier);
            List<Long> keys = new ArrayList<>(buckets.keySet());
            Collections.sort(keys);
            for (long key : keys) {
                if (key * tier.windowMs + tier.windowMs <= fromMs) continue;
                Bucket b = buckets.get(key);
                for (int i = 0; i < slotsPerBucket; i++) {
                    byte[] slot = b.read(i);
                    if (SlotCodec.isOccupied(slot) && !SlotCodec.isTorn(slot)) {
                        Intent it = SlotCodec.decode(slot);
                        if (it.getExecuteAt().toEpochMilli() >= fromMs) acc.add(it);
                    }
                }
            }
        }
        return acc.iterator();
    }

    public java.util.Iterator<SlotEntry> scanSlotsFrom(java.time.Instant from) {
        long fromMs = from.toEpochMilli();
        java.util.List<SlotEntry> acc = new java.util.ArrayList<>();
        for (WheelTier tier : WheelTier.values()) {
            ConcurrentHashMap<Long, Bucket> buckets = wheels.get(tier);
            java.util.List<Long> keys = new java.util.ArrayList<>(buckets.keySet());
            java.util.Collections.sort(keys);
            for (long key : keys) {
                Bucket b = buckets.get(key);
                for (int i = 0; i < slotsPerBucket; i++) {
                    byte[] slot = b.read(i);
                    if (SlotCodec.isOccupied(slot) && !SlotCodec.isTorn(slot)) {
                        Intent it = SlotCodec.decode(slot);
                        SlotLocation loc = new SlotLocation(tier, key, i, false);
                        acc.add(new SlotEntry(loc, it));
                    }
                }
            }
        }
        // 过滤 by executeAt(返回全部,由调用方按时间筛)
        return acc.iterator();
    }

    public void forceDirty() {
        for (var buckets : wheels.values()) {
            for (Bucket b : buckets.values()) {
                if (!b.closed && b.hasUnflushed()) {
                    long before = b.writeCount.get();
                    b.seg.force();
                    b.flushedWriteCount = before;
                }
            }
        }
    }

    public long getMinTailExecuteAt() { return clock.getAsLong() + horizonMs; }

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
        if (ch.size() < bucketBytes) ch.truncate(bucketBytes);
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
        private final AtomicLong writeCount = new AtomicLong();
        private volatile long flushedWriteCount = 0;
        private volatile boolean closed = false;
        Bucket(WheelTier t, long k, Path p, FileChannel ch, MemorySegment m) { tier=t; bucketKey=k; path=p; channel=ch; seg=m; }
        int alloc() {
            int idx = next.getAndIncrement();
            if (idx >= slotsPerBucket) throw new IllegalStateException("bucket overflow: " + tier + "/" + bucketKey);
            return idx;
        }
        void write(int slot, byte[] data) {
            long off = (long) slot * SlotCodec.SLOT_SIZE;
            MemorySegment src = MemorySegment.ofArray(data);
            MemorySegment.copy(src, 0, seg, off, data.length);
            writeCount.incrementAndGet();
        }
        byte[] read(int slot) {
            long off = (long) slot * SlotCodec.SLOT_SIZE;
            if (off + SlotCodec.SLOT_SIZE > seg.byteSize()) return new byte[SlotCodec.SLOT_SIZE];
            byte[] buf = new byte[SlotCodec.SLOT_SIZE];
            MemorySegment.copy(seg, off, MemorySegment.ofArray(buf), 0, SlotCodec.SLOT_SIZE);
            return buf;
        }
        /** 启动恢复:扫描槽位找到第一个空槽(高水位),设为 next,防重启覆写。 */
        void recoverHighWaterMark() {
            int high = 0;
            for (int i = 0; i < slotsPerBucket; i++) {
                byte[] slot = read(i);
                if (!SlotCodec.isOccupied(slot)) { high = i; break; }
                high = i + 1;
            }
            next.set(high);
        }
        boolean hasUnflushed() { return writeCount.get() > flushedWriteCount; }
        void close() {
            if (closed) return;
            closed = true;
            try { seg.force(); } catch (Exception ignored) {}
            try { channel.close(); } catch (IOException ignored) {}
        }
    }
}
