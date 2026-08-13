package com.loomq.infrastructure.wheel;

/**
 * 写入已关闭桶时抛出。触发场景:deleteBucket(回收)与在途 put 的竞态——put 已取到桶引用,
 * 桶被 close 后写入映射段会静默成功、字节随文件删除而丢失。write() 在 forceLock readLock
 * 内检测 closed 抛此异常,put() 捕获后移除死桶引用、重试进新桶(有界),绝不静默丢失。
 */
public class BucketClosedException extends RuntimeException {
    public BucketClosedException(String message) { super(message); }
}
