package com.loomq.raft;

import com.loomq.common.RaftRole;
import com.loomq.spi.WalAccessor;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoordinationV1Api;
import io.kubernetes.client.openapi.models.V1Lease;
import io.kubernetes.client.openapi.models.V1LeaseSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.util.Config;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * K8s Lease 选主实现。
 *
 * <p>使用 Kubernetes Lease API 实现 leader election。
 * epoch 对应 Lease.leaseTransitions。
 *
 * <p>行为：
 * <ul>
 *   <li>启动时尝试创建/更新 Lease</li>
 *   <li>成功 → 成为 Leader，epoch = leaseTransitions</li>
 *   <li>失败（已有 holder）→ 成为 Follower，定期重试</li>
 *   <li>Leader 定期续约 Lease</li>
 *   <li>Follower 定期检查 Lease 状态</li>
 * </ul>
 */
public class K8sLeaseElection implements LeaderElection {
    private static final Logger log = LoggerFactory.getLogger(K8sLeaseElection.class);

    private final K8sLeaseConfig config;
    private final WalAccessor wal;
    private final CoordinationV1Api coordinationApi;
    private final ScheduledExecutorService scheduler;
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile String currentLeader = null;
    private volatile long currentEpoch = 0;
    private volatile ScheduledFuture<?> renewTask;
    private volatile long lastRenewNanoTime = 0;
    private volatile boolean stopped = false;
    private final long clockSkewBufferSeconds;

    private final List<Consumer<Long>> onBecomeLeaderListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<Long>> onBecomeFollowerListeners = new CopyOnWriteArrayList<>();

    public K8sLeaseElection(K8sLeaseConfig config, WalAccessor wal) {
        this(config, wal, createDefaultApiClient());
    }

    public K8sLeaseElection(K8sLeaseConfig config, WalAccessor wal, ApiClient apiClient) {
        this.config = config;
        this.wal = wal;
        this.coordinationApi = new CoordinationV1Api(apiClient);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "k8s-lease-" + config.podName());
            t.setDaemon(true);
            return t;
        });
        this.clockSkewBufferSeconds = config.clockSkewBufferSeconds();

        // Restore persisted epoch
        long persistedEpoch = wal.getLastLogEpoch();
        if (persistedEpoch > 0) {
            this.currentEpoch = persistedEpoch;
        }
    }

    private static ApiClient createDefaultApiClient() {
        try {
            return Config.defaultClient();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create default K8s API client", e);
        }
    }

    @Override
    public RaftRole role() {
        return role;
    }

    @Override
    public boolean isLeader() {
        return role == RaftRole.LEADER;
    }

    private boolean isLeaseExpiredMonotonic() {
        if (lastRenewNanoTime == 0) return false;
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastRenewNanoTime);
        return elapsedMs > config.leaseDurationSeconds() * 1000L;
    }

    @Override
    public long currentEpoch() {
        return currentEpoch;
    }

    @Override
    public String currentLeader() {
        return currentLeader;
    }

    @Override
    public void start() {
        log.info("K8sLeaseElection starting: pod={}, namespace={}, lease={}",
            config.podName(), config.namespace(), config.leaseName());

        // Try to acquire lease immediately
        tryAcquireLease();

        // Schedule periodic lease management
        long intervalMs = config.renewIntervalSeconds() * 1000L;
        renewTask = scheduler.scheduleAtFixedRate(this::tryAcquireLease,
            intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        stopped = true;
        log.info("K8sLeaseElection stopping: pod={}", config.podName());
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        role = RaftRole.FOLLOWER;
    }

    @Override
    public synchronized void onAppendEntries(long leaderEpoch, String leaderId) {
        if (stopped) return;
        if (leaderEpoch >= currentEpoch) {
            if (role != RaftRole.FOLLOWER) {
                becomeFollower(leaderId, leaderEpoch);
            } else {
                currentLeader = leaderId;
                if (leaderEpoch > currentEpoch) {
                    currentEpoch = leaderEpoch;
                    persistEpoch(leaderEpoch);
                }
            }
        }
    }

    @Override
    public void addBecomeLeaderListener(Consumer<Long> listener) {
        onBecomeLeaderListeners.add(listener);
    }

    @Override
    public void addBecomeFollowerListener(Consumer<Long> listener) {
        onBecomeFollowerListeners.add(listener);
    }

    /**
     * 尝试获取或续约 Lease。
     *
     * <p>逻辑：
     * <ol>
     *   <li>读取当前 Lease</li>
     *   <li>如果无 Lease 或 holder 是自己 → 创建/更新 Lease</li>
     *   <li>如果 holder 是别人且未过期 → 成为 Follower</li>
     *   <li>如果 holder 是别人但已过期 → 抢占 Lease</li>
     * </ol>
     */
    private void tryAcquireLease() {
        if (stopped) return;
        try {
            if (role == RaftRole.LEADER && isLeaseExpiredMonotonic()) {
                log.warn("Lease expired (monotonic clock), stepping down");
                stepDown(currentEpoch);
                return;
            }

            V1Lease existingLease = readLease();

            if (existingLease == null) {
                // No lease exists — try to create it
                createLease();
                return;
            }

            String holder = existingLease.getSpec() != null
                ? existingLease.getSpec().getHolderIdentity()
                : null;
            boolean isExpired = isLeaseExpired(existingLease);

            if (holder == null || holder.equals(config.podName()) || isExpired) {
                // Try to acquire: either no holder, we already hold it, or it's expired
                updateLease(existingLease);
            } else {
                // Someone else holds the lease — become follower
                long leaseTransitions = existingLease.getSpec().getLeaseTransitions() != null
                    ? existingLease.getSpec().getLeaseTransitions() : 0;
                becomeFollower(holder, leaseTransitions);
            }
        } catch (ApiException e) {
            log.warn("K8s Lease API error: code={}, body={}", e.getCode(), e.getResponseBody());
        } catch (Exception e) {
            log.error("Unexpected error in lease management", e);
        }
    }

    private V1Lease readLease() throws ApiException {
        try {
            return coordinationApi.readNamespacedLease(
                config.leaseName(), config.namespace()).execute();
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return null;
            }
            throw e;
        }
    }

    private void createLease() {
        try {
            OffsetDateTime now = OffsetDateTime.now();
            V1Lease lease = new V1Lease()
                .metadata(new V1ObjectMeta()
                    .name(config.leaseName())
                    .namespace(config.namespace()))
                .spec(new V1LeaseSpec()
                    .holderIdentity(config.podName())
                    .leaseDurationSeconds(config.leaseDurationSeconds())
                    .acquireTime(now)
                    .renewTime(now)
                    .leaseTransitions((int) 0));

            coordinationApi.createNamespacedLease(config.namespace(), lease).execute();
            updateFencingState();
            becomeLeader(0);
            log.info("Created Lease: holder={}, transitions={}", config.podName(), 0);
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                // Conflict — someone else created it first
                log.debug("Lease creation conflict (409), will retry");
            } else {
                log.warn("Failed to create Lease: code={}, body={}", e.getCode(), e.getResponseBody());
            }
        }
    }

    private void updateLease(V1Lease existingLease) {
        try {
            long existingTransitions = existingLease.getSpec().getLeaseTransitions() != null
                ? existingLease.getSpec().getLeaseTransitions() : 0;
            boolean wasOurs = config.podName().equals(existingLease.getSpec().getHolderIdentity());
            long newTransitions = wasOurs ? existingTransitions : existingTransitions + 1;

            existingLease.getSpec()
                .holderIdentity(config.podName())
                .renewTime(OffsetDateTime.now())
                .leaseTransitions((int) newTransitions);

            coordinationApi.replaceNamespacedLease(
                config.leaseName(), config.namespace(), existingLease).execute();
            updateFencingState();

            becomeLeader(newTransitions);
            log.info("Updated Lease: holder={}, transitions={}", config.podName(), newTransitions);
        } catch (ApiException e) {
            if (e.getCode() == 409) {
                log.debug("Lease update conflict (409), will retry");
            } else {
                log.warn("Failed to update Lease: code={}, body={}", e.getCode(), e.getResponseBody());
            }
        }
    }

    private boolean isLeaseExpired(V1Lease lease) {
        if (lease.getSpec() == null || lease.getSpec().getRenewTime() == null) {
            return true;
        }
        OffsetDateTime renewTime = lease.getSpec().getRenewTime();
        int durationSeconds = lease.getSpec().getLeaseDurationSeconds() != null
            ? lease.getSpec().getLeaseDurationSeconds()
            : config.leaseDurationSeconds();
        // Wall-clock comparison with safety buffer for clock skew
        OffsetDateTime expiryTime = renewTime.plusSeconds(durationSeconds)
                                             .minusSeconds(clockSkewBufferSeconds);
        return OffsetDateTime.now().isAfter(expiryTime);
    }

    private void updateFencingState() {
        lastRenewNanoTime = System.nanoTime();
    }

    /**
     * Central role transition. All role changes go through here.
     *
     * @param target the desired role
     * @param leader the leader ID (null for stepDown)
     * @param epoch the new epoch
     */
    private synchronized void transitionRole(RaftRole target, String leader, long epoch) {
        if (stopped) return;
        long safeEpoch = Math.max(epoch, currentEpoch);
        if (role != target) {
            role = target;
            currentLeader = leader;
            currentEpoch = safeEpoch;
            persistEpoch(safeEpoch);
            if (target == RaftRole.LEADER) {
                log.info("Became LEADER: pod={}, epoch={}", config.podName(), safeEpoch);
                for (Consumer<Long> listener : onBecomeLeaderListeners) {
                    listener.accept(safeEpoch);
                }
            } else {
                log.info("Became FOLLOWER: pod={}, leader={}, epoch={}", config.podName(), leader, safeEpoch);
                for (Consumer<Long> listener : onBecomeFollowerListeners) {
                    listener.accept(safeEpoch);
                }
            }
        } else {
            // Already in target role — just update leader/epoch if changed
            if (leader != null) {
                currentLeader = leader;
            }
            if (safeEpoch != currentEpoch) {
                currentEpoch = safeEpoch;
                persistEpoch(safeEpoch);
            }
        }
    }

    private synchronized void becomeLeader(long epoch) {
        transitionRole(RaftRole.LEADER, config.podName(), epoch);
    }

    private synchronized void becomeFollower(String leader, long epoch) {
        transitionRole(RaftRole.FOLLOWER, leader, epoch);
    }

    /**
     * Step-down to follower with a new epoch.
     * Called when a heartbeat response reveals a higher epoch on a follower.
     */
    @Override
    public void stepDown(long newEpoch) {
        becomeFollower(null, newEpoch);
    }

    private void persistEpoch(long epoch) {
        try {
            wal.setCurrentEpoch(epoch);
            wal.setVotedFor(null);
            wal.persistRaftMeta();
        } catch (Exception e) {
            log.error("Failed to persist epoch {}", epoch, e);
        }
    }
}
