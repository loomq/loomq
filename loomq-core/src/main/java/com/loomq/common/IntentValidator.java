package com.loomq.common;

import com.loomq.domain.intent.Intent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Objects;

/**
 * Intent 入口校验器。
 *
 * 在 createIntent / createIntents 入口校验 Intent 字段，
 * 防止 NPE 和 SlotOverflowException 等深层错误。
 * 校验失败抛出 {@link IllegalArgumentException}。
 *
 * @author loomq
 */
public final class IntentValidator {

    /** 槽格式 intentId 最大字节数（SlotCodec 限制） */
    static final int MAX_INTENT_ID_BYTES = 24;

    private IntentValidator() {
    }

    /**
     * 校验 Intent 的必填字段与约束。
     *
     * @param intent 待校验的 Intent
     * @throws IllegalArgumentException 校验失败
     */
    public static void validate(Intent intent) {
        Objects.requireNonNull(intent, "intent cannot be null");

        Instant executeAt = intent.getExecuteAt();
        if (executeAt == null) {
            throw new IllegalArgumentException("executeAt is required");
        }

        Instant deadline = intent.getDeadline();
        if (deadline != null && !deadline.isAfter(executeAt)) {
            throw new IllegalArgumentException("deadline must be after executeAt");
        }

        String intentId = intent.getIntentId();
        if (intentId != null && intentId.isBlank()) {
            throw new IllegalArgumentException("intentId must not be blank");
        }

        if (intentId != null) {
            int byteLen = intentId.getBytes(StandardCharsets.UTF_8).length;
            if (byteLen > MAX_INTENT_ID_BYTES) {
                throw new IllegalArgumentException(
                    "intentId exceeds " + MAX_INTENT_ID_BYTES + " bytes (got " + byteLen + ")");
            }
        }
    }
}
