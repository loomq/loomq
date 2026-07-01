package com.loomq.infrastructure.wheel;

import com.loomq.domain.intent.Intent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 无界有序尾:按 executeAtMs 排序存储超出 day 视界的 Intent。
 *
 * <p><b>持久化源 = append-only run 文件</b> {@code <dataDir>/tail/tail.log}。内存 map
 * 仅是构造时 {@link #loadRun()} 重放 run 文件重建的缓存,不是真相源。DURABLE 的
 * createIntent 在 tail 上的写入必须先 append 到 run 文件,再由 GroupCommitBarrier
 * 调用 {@link #flush()} 强制落盘后才算持久——否则崩溃会丢失 &gt;30 天的远期 Intent,
 * 违反 DURABLE 契约。</p>
 *
 * <p>Run 记录格式(big-endian):
 * <pre>{@code
 * type(1) | execMs(8) | idLen(1) | intentId(idLen) | [encodedSlot(256) if type==PUT]
 * }</pre>
 * type=1 PUT(含 256B encodedSlot),type=0 TOMBSTONE(仅头部 + intentId)。</p>
 *
 * <p><b>同毫秒冲突修复</b>:byExecuteAt 的 value 是 {@code Set<String>},同一 executeAtMs
 * 的多个 intentId 共存于一个集合,不再像旧版按 ms 直接做 skip-list 键而互相覆盖。</p>
 *
 * <p><b>已知限制(v1)</b>:无 compaction,run 文件随 put+tombstone 单调增长(spec §11 延后)。
 * 崩溃恢复依赖 {@link #flush()} 已强制到磁盘的记录;未 flush 的尾部记录可能丢失——这与
 * DURABLE 契约一致(返回 DURABLE 前 barrier 必已 flush)。</p>
 */
public final class TailIndex implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TailIndex.class);

    private static final byte TYPE_TOMBSTONE = 0;
    private static final byte TYPE_PUT = 1;

    private final LongSupplier clock;
    private final Path runFile;
    private final FileChannel runChannel;

    /** 真相源的内存镜像:intentId → (executeAtMs, encodedSlot)。 */
    private final ConcurrentHashMap<String, TailRecord> byId = new ConcurrentHashMap<>();
    /** executeAtMs → 同毫秒 intentId 集合,按 ms 升序遍历。 */
    private final ConcurrentSkipListMap<Long, Set<String>> byExecuteAt = new ConcurrentSkipListMap<>();

    /** 序列化 put/remove(内存变更 + run 追加)以保证重放顺序与内存顺序一致、记录不撕裂。 */
    private final Object appendLock = new Object();

    public TailIndex(Path dataDir, LongSupplier clock) {
        this.clock = clock;
        this.runFile = dataDir.resolve("tail").resolve("tail.log");
        try {
            Files.createDirectories(dataDir.resolve("tail"));
            // 重放已存在的 run 文件(若有)重建内存态,先于打开写句柄。返回最后一条有效记录
            // 的字节偏移(validLen);若尾部存在撕裂记录(崩溃 mid-append),validLen < 文件大小。
            long validLen = loadRun();
            // 截断撕裂的尾部字节:若不截断,后续 APPEND 写入会落在撕裂字节之后,下次重启时
            // loadRun 会把撕裂头 + 后续合法字节误解析为一条垃圾 PUT(幻影条目 + 丢失真实条目)。
            truncateTornTail(validLen);
            this.runChannel = FileChannel.open(
                runFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("init tail run file failed: " + runFile, e);
        }
    }

    /**
     * 若 {@code validLen < 文件大小},说明尾部存在撕裂记录(崩溃 mid-append 产生的部分写):
     * 以 WRITE(非 APPEND)打开 run 文件并 {@code truncate(validLen)} 截断到上一条有效记录边界。
     * 截断后再打开 APPEND 通道,使下一次写入覆盖撕裂字节、下次重放只看到有效记录。
     *
     * <p>文件不存在/为空时 no-op({@code validLen == 0} 且文件未创建)。截断失败不阻止启动
     * (尽力而为,记录告警)——但 run 文件是真相源,继续 APPEND 可能污染日志,操作者应排查。</p>
     */
    private void truncateTornTail(long validLen) {
        if (validLen < 0) validLen = 0;
        if (!Files.isRegularFile(runFile)) return; // 新文件,无撕裂尾部可截断
        try (FileChannel ch = FileChannel.open(runFile, StandardOpenOption.WRITE)) {
            long size = ch.size();
            if (size > validLen) {
                ch.truncate(validLen);
                log.warn("truncated torn tail run file from {} to {} bytes (removed {} torn bytes)",
                    size, validLen, size - validLen);
            }
        } catch (IOException e) {
            log.warn("truncate torn tail run file failed; continuing with possible torn trailing bytes: {}",
                runFile, e);
        }
    }

    /** 存入 tail 并追加 PUT 记录到 run 文件(未 force,由 {@link #flush()} 统一强制)。 */
    public void put(Intent intent) {
        byte[] enc = SlotCodec.encode(intent);
        long execMs = intent.getExecuteAt().toEpochMilli();
        String intentId = intent.getIntentId();
        synchronized (appendLock) {
            // 先 append 再 applyPut:若 append 抛出(IOException mid-write 或 intentId 过长校验),
            // 内存不被污染(无幻影条目),也不会留下"已 mutate 内存但记录撕裂"的悬挂状态。
            appendRecord(TYPE_PUT, intentId, execMs, enc);
            applyPut(intentId, execMs, enc);
        }
    }

    /** 从 tail 移除并追加 TOMBSTONE 记录。返回是否曾存在。 */
    public boolean remove(String intentId) {
        synchronized (appendLock) {
            TailRecord r = byId.get(intentId);
            if (r == null) return false; // 短路:不存在则跳过 tombstone 追加,避免孤儿 tombstone
            // 先 append TOMBSTONE 再 applyRemove:若 append 抛出,内存不被误删(无数据丢失)。
            appendRecord(TYPE_TOMBSTONE, intentId, r.executeAtMs(), null);
            applyRemove(intentId);
            return true;
        }
    }

    /** 从 fromMs(含)起按 executeAtMs 升序扫描 tail 项(快照迭代器)。 */
    public Iterator<TailEntry> scanFrom(long fromMs) {
        List<TailEntry> acc = new ArrayList<>();
        for (var e : byExecuteAt.tailMap(fromMs, true).entrySet()) {
            long execMs = e.getKey();
            for (String id : e.getValue()) {
                TailRecord r = byId.get(id);
                if (r != null) acc.add(new TailEntry(execMs, r.encodedSlot()));
            }
        }
        return acc.iterator();
    }

    /**
     * 把进入 day 视界的 entry 写入 {@code store} 并从 tail 移除(追加 tombstone)。返回提升数。
     *
     * <p><b>并发</b>:逐条持有 {@code appendLock},快照后原子执行 decode→store.put→
     * appendTombstone→applyRemove,使每条提升对并发 {@link #put}/{@link #remove} 原子。
     * {@code store.put} 是 mmap memcpy(无 I/O),持锁期间开销可忽略。</p>
     *
     * <p><b>崩溃窗口(v1 已知限制,spec §11 接受)</b>:若进程在 {@code store.put}(已分配
     * wheel 槽)与 tombstone 追加之间崩溃,recovery 会重新提升该 intentId → 同一 intentId
     * 在 wheel 中分配第二个槽。{@code WheelRecovery}(Task 8)按 intentId 去重(取最大
     * revision),故不会重复投递——仅浪费一个过期后即删的桶槽。v1 选择"宁可多分配,
     * 不可丢失"(prefers over-loss)。</p>
     */
    public int promoteInto(WheelStore store) {
        long horizon = store.getMinTailExecuteAt();
        int promoted = 0;
        // 快照键集,避免 promote 过程中 remove 改动 byExecuteAt 触发 CME
        List<Long> keys = new ArrayList<>(byExecuteAt.headMap(horizon, true).keySet());
        for (long execMs : keys) {
            Set<String> ids = byExecuteAt.get(execMs);
            if (ids == null) continue;
            for (String intentId : List.copyOf(ids)) {
                synchronized (appendLock) {
                    TailRecord r = byId.get(intentId);
                    if (r == null) continue; // 并发 remove 已摘除
                    if (r.executeAtMs() > horizon) continue; // 并发 re-PUT 到更远的 ms
                    Intent it = SlotCodec.decode(r.encodedSlot());
                    store.put(it); // mmap memcpy,持锁期间开销可忽略
                    appendRecord(TYPE_TOMBSTONE, intentId, r.executeAtMs(), null);
                    applyRemove(intentId);
                    promoted++;
                }
            }
        }
        if (promoted > 0) log.debug("Promoted {} tail entries into day wheel", promoted);
        return promoted;
    }

    /** 强制 run 文件缓冲写落盘(GroupCommitBarrier 在 ack DURABLE 写者前调用)。 */
    public void flush() {
        try {
            runChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException("flush tail run file failed: " + runFile, e);
        }
    }

    public int size() {
        return byId.size();
    }

    @Override
    public void close() {
        try {
            runChannel.close();
        } catch (IOException e) {
            log.warn("close tail run channel failed", e);
        }
    }

    // ===== run 文件重放 =====

    /**
     * 从 run 文件起始逐条重放到正确终态。PUT → 入内存;TOMBSTONE → 撤内存。
     * 文件不存在或为空时 no-op。尾部撕裂记录(长度不足)按"截断"处理,停止重放。
     *
     * @return 最后一条有效记录的结束字节偏移(validLen);之后的字节(若有)是撕裂尾部,
     *     需由调用方截断,否则下次 APPEND 会污染日志。
     */
    private long loadRun() {
        if (!Files.isRegularFile(runFile)) return 0L;
        byte[] all;
        try {
            all = Files.readAllBytes(runFile);
        } catch (IOException e) {
            throw new RuntimeException("read tail run file failed: " + runFile, e);
        }
        int pos = 0;
        int len = all.length;
        while (pos < len) {
            int headerEnd = pos + 1 + 8 + 1; // type + execMs + idLen
            if (headerEnd > len) break; // 撕裂头部
            byte type = all[pos];
            long execMs = ByteBuffer.wrap(all, pos + 1, 8).order(ByteOrder.BIG_ENDIAN).getLong();
            int idLen = all[pos + 1 + 8] & 0xFF;
            int idStart = headerEnd;
            int afterId = idStart + idLen;
            if (afterId > len) break; // 撕裂 intentId
            String intentId = new String(all, idStart, idLen, StandardCharsets.UTF_8);
            if (type == TYPE_PUT) {
                int afterSlot = afterId + SlotCodec.SLOT_SIZE;
                if (afterSlot > len) break; // 撕裂 encodedSlot
                byte[] enc = new byte[SlotCodec.SLOT_SIZE];
                System.arraycopy(all, afterId, enc, 0, SlotCodec.SLOT_SIZE);
                applyPut(intentId, execMs, enc);
                pos = afterSlot;
            } else if (type == TYPE_TOMBSTONE) {
                applyRemove(intentId);
                pos = afterId;
            } else {
                break; // 未知记录类型,按截断停止
            }
        }
        // pos 即最后一条有效记录的结束字节偏移;之后的字节(若有)是撕裂尾部,需截断。
        return pos;
    }

    // ===== 内存态变更(put/remove 与 loadRun 共用,保证重放语义一致) =====

    private void applyPut(String intentId, long execMs, byte[] enc) {
        TailRecord old = byId.put(intentId, new TailRecord(execMs, enc));
        if (old != null) {
            // 同 id 重复 put:先从旧 ms 集合摘除,避免遗留脏索引
            removeFromExecuteAt(old.executeAtMs(), intentId);
        }
        byExecuteAt.computeIfAbsent(execMs, k -> ConcurrentHashMap.<String>newKeySet()).add(intentId);
    }

    private void applyRemove(String intentId) {
        TailRecord r = byId.remove(intentId);
        if (r == null) return; // 孤儿 tombstone(idempotent remove),no-op
        removeFromExecuteAt(r.executeAtMs(), intentId);
    }

    private void removeFromExecuteAt(long execMs, String intentId) {
        byExecuteAt.computeIfPresent(execMs, (k, set) -> {
            set.remove(intentId);
            return set.isEmpty() ? null : set; // 返回 null 即从 map 移除该键
        });
    }

    // ===== run 文件追加 =====

    private void appendRecord(byte type, String intentId, long execMs, byte[] encodedSlot) {
        byte[] idBytes = intentId.getBytes(StandardCharsets.UTF_8);
        if (idBytes.length > 0xFF) {
            throw new IllegalArgumentException("intentId too long for run record: " + idBytes.length);
        }
        int slotLen = (type == TYPE_PUT) ? SlotCodec.SLOT_SIZE : 0;
        ByteBuffer buf = ByteBuffer.allocate(1 + 8 + 1 + idBytes.length + slotLen).order(ByteOrder.BIG_ENDIAN);
        buf.put(type);
        buf.putLong(execMs);
        buf.put((byte) idBytes.length);
        buf.put(idBytes);
        if (type == TYPE_PUT) buf.put(encodedSlot);
        buf.flip();
        try {
            while (buf.hasRemaining()) runChannel.write(buf);
        } catch (IOException e) {
            throw new RuntimeException("append tail run record failed: " + runFile, e);
        }
    }

    /** 内存中的 tail 项:executeAtMs + 定长 encodedSlot。 */
    private record TailRecord(long executeAtMs, byte[] encodedSlot) {}
}
