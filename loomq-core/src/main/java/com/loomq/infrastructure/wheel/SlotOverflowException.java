package com.loomq.infrastructure.wheel;

/** Intent 编码后超出定长槽 payload 区(210B)时抛出。Blob 外置为 deferred 项。 */
public class SlotOverflowException extends RuntimeException {
    public SlotOverflowException(String message) { super(message); }
}
