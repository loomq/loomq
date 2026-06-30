package com.loomq.raft;

/**
 * K8s Lease 选主配置。
 *
 * @param leaseDurationSeconds    Lease 过期时间（秒）
 * @param renewIntervalSeconds    续约间隔（秒），应小于 leaseDurationSeconds/3
 * @param namespace               Lease 所在 namespace
 * @param leaseName               Lease 资源名称
 * @param podName                 当前 Pod 名称（用于 holderIdentity）
 * @param clockSkewBufferSeconds  时钟偏移安全缓冲（秒），leaseDuration 必须大于 renewInterval + buffer
 */
public record K8sLeaseConfig(
    int leaseDurationSeconds,
    int renewIntervalSeconds,
    String namespace,
    String leaseName,
    String podName,
    long clockSkewBufferSeconds
) {
    public K8sLeaseConfig(int leaseDurationSeconds, int renewIntervalSeconds,
                          String namespace, String leaseName, String podName) {
        this(leaseDurationSeconds, renewIntervalSeconds, namespace, leaseName, podName, 5);
    }

    public K8sLeaseConfig {
        if (leaseDurationSeconds <= 0) {
            throw new IllegalArgumentException("leaseDurationSeconds must be positive");
        }
        if (renewIntervalSeconds <= 0) {
            throw new IllegalArgumentException("renewIntervalSeconds must be positive");
        }
        if (renewIntervalSeconds >= leaseDurationSeconds) {
            throw new IllegalArgumentException("renewIntervalSeconds must be less than leaseDurationSeconds");
        }
        if (namespace == null || namespace.isBlank()) {
            throw new IllegalArgumentException("namespace cannot be blank");
        }
        if (leaseName == null || leaseName.isBlank()) {
            throw new IllegalArgumentException("leaseName cannot be blank");
        }
        if (podName == null || podName.isBlank()) {
            throw new IllegalArgumentException("podName cannot be blank");
        }
        if (clockSkewBufferSeconds < 0) {
            throw new IllegalArgumentException("clockSkewBufferSeconds must be >= 0");
        }
        if (leaseDurationSeconds <= renewIntervalSeconds + clockSkewBufferSeconds) {
            throw new IllegalArgumentException(
                "leaseDurationSeconds must be > renewIntervalSeconds + clockSkewBufferSeconds");
        }
    }
}
