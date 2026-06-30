package com.loomq.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.loomq.common.RaftRole;
import com.loomq.config.WalConfig;
import com.loomq.infrastructure.wal.SimpleWalWriter;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.JSON;
import io.kubernetes.client.openapi.models.V1Lease;
import io.kubernetes.client.openapi.models.V1LeaseSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("slow")
class K8sLeaseElectionTest {
    private Path dataDir;
    private SimpleWalWriter wal;

    @BeforeEach
    void setUp() throws Exception {
        dataDir = Files.createTempDirectory("k8s-lease-test-");
        WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
            "memory_segment", 1, 8, 64, 10, 4, 1, false);
        wal = new SimpleWalWriter(cfg, "k8s-lease-test");
    }

    @AfterEach
    void tearDown() {
        if (wal != null) wal.close();
    }

    @Test
    void standaloneModeShouldAlwaysBeLeader() {
        // StandaloneElection is the non-K8s fallback
        StandaloneElection election = new StandaloneElection();
        election.start();
        assertTrue(election.isLeader());
        assertEquals(RaftRole.LEADER, election.role());
        assertEquals(1L, election.currentEpoch());
        assertEquals("standalone", election.currentLeader());
        election.stop();
    }

    @Test
    void standaloneShouldNotifyLeaderListener() {
        AtomicBoolean notified = new AtomicBoolean(false);
        StandaloneElection election = new StandaloneElection();
        election.addBecomeLeaderListener(epoch -> {
            assertEquals(1L, epoch);
            notified.set(true);
        });
        election.start();
        assertTrue(notified.get(), "should notify leader listener on start");
        election.stop();
    }

    @Test
    void k8sConfigShouldValidate() {
        // Valid config
        new K8sLeaseConfig(10, 3, "default", "loomq-leader", "pod-1");

        // Invalid: negative duration
        try {
            new K8sLeaseConfig(-1, 3, "default", "loomq-leader", "pod-1");
            assertTrue(false, "should throw on negative duration");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Invalid: renew >= duration
        try {
            new K8sLeaseConfig(10, 10, "default", "loomq-leader", "pod-1");
            assertTrue(false, "should throw when renew >= duration");
        } catch (IllegalArgumentException e) {
            // expected
        }

        // Invalid: blank namespace
        try {
            new K8sLeaseConfig(10, 3, "", "loomq-leader", "pod-1");
            assertTrue(false, "should throw on blank namespace");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    void k8sElectionShouldPersistEpoch() {
        // Test that epoch persistence works through WalAccessor
        wal.setCurrentEpoch(42L);
        wal.persistRaftMeta();

        // Create a new WAL writer from the same directory
        try {
            wal.close();
            WalConfig cfg = new WalConfig(dataDir.toString(), 1, "batch", 100, false,
                "memory_segment", 1, 8, 64, 10, 4, 1, false);
            SimpleWalWriter wal2 = new SimpleWalWriter(cfg, "k8s-lease-test");
            try {
                assertEquals(42, wal2.getLastLogEpoch(),
                    "epoch should persist across WAL close/reopen");
            } finally {
                wal2.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void leaderElectionInterfaceShouldBeComplete() {
        // Verify that all LeaderElection interface methods are implementable
        LeaderElection election = new StandaloneElection();
        election.start();
        assertTrue(election.isLeader());
        assertEquals(RaftRole.LEADER, election.role());
        assertEquals(1L, election.currentEpoch());
        assertEquals("standalone", election.currentLeader());
        election.onAppendEntries(1L, "other"); // no-op for standalone
        election.stop();
    }

    @Test
    void configValidationShouldRejectInvalidClockSkewBuffer() {
        // leaseDuration=10, renewInterval=3, buffer=8 → 10 <= 3+8 → should throw
        assertThrows(IllegalArgumentException.class, () ->
            new K8sLeaseConfig(10, 3, "default", "loomq-leader", "pod-1", 8));
    }

    @Test
    void defaultClockSkewBufferShouldBeFive() {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
        assertEquals(5, config.clockSkewBufferSeconds());
    }

    @Test
    void tryAcquireLeaseShouldExitAfterStepDown() throws Exception {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");

        // Valid lease: holder is self, recent renewTime (not wall-clock expired).
        // Without the `return;` fix, stepDown() sets FOLLOWER, then the code falls through
        // to readLease() -> finds self-held lease -> updateLease() -> becomeLeader() -> LEADER.
        // With the `return;` fix, the method exits after stepDown() -> stays FOLLOWER.
        V1Lease validLease = new V1Lease()
            .metadata(new V1ObjectMeta().name("loomq-leader").namespace("default"))
            .spec(new V1LeaseSpec()
                .holderIdentity("pod-1")
                .renewTime(OffsetDateTime.now())
                .leaseDurationSeconds(15)
                .leaseTransitions(0));

        // Intercept at the OkHttp layer: the K8s CoordinationV1Api uses ApiClient's
        // OkHttpClient internally. By injecting a custom interceptor, we can make
        // readLease() and updateLease() succeed without mocking any K8s classes.
        JSON json = new JSON();
        String leaseJson = json.serialize(validLease);
        AtomicBoolean k8sApiCalled = new AtomicBoolean(false);

        Interceptor mockInterceptor = chain -> {
            k8sApiCalled.set(true);
            return new Response.Builder()
                .request(chain.request())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .body(ResponseBody.create(leaseJson, MediaType.parse("application/json")))
                .build();
        };

        OkHttpClient mockHttpClient = new OkHttpClient.Builder()
            .addInterceptor(mockInterceptor)
            .build();
        ApiClient apiClient = new ApiClient(mockHttpClient);
        // serverIndex defaults to 0 with an empty server URL, producing relative URLs
        // that fail before reaching OkHttp. setBasePath sets serverIndex=null so
        // buildUrl falls back to basePath ("http://localhost") as the base.
        apiClient.setBasePath("http://localhost");

        K8sLeaseElection election = new K8sLeaseElection(config, wal, apiClient);
        try {
            // Force into LEADER state with stale monotonic clock
            Field roleField = K8sLeaseElection.class.getDeclaredField("role");
            roleField.setAccessible(true);
            roleField.set(election, RaftRole.LEADER);

            Field epochField = K8sLeaseElection.class.getDeclaredField("currentEpoch");
            epochField.setAccessible(true);
            epochField.set(election, 1L);

            Field nanoField = K8sLeaseElection.class.getDeclaredField("lastRenewNanoTime");
            nanoField.setAccessible(true);
            // 60 seconds ago — far beyond 15s lease duration
            nanoField.set(election, System.nanoTime() - TimeUnit.SECONDS.toNanos(60));

            java.lang.reflect.Method tryAcquire = K8sLeaseElection.class.getDeclaredMethod("tryAcquireLease");
            tryAcquire.setAccessible(true);

            // Monotonic expired -> stepDown -> role becomes FOLLOWER
            // Without `return;`: falls through -> readLease succeeds -> updateLease -> becomeLeader -> LEADER
            //   (k8sApiCalled would be true, role would be LEADER)
            // With `return;`: exits method -> stays FOLLOWER
            //   (k8sApiCalled stays false, role stays FOLLOWER)
            tryAcquire.invoke(election);

            assertEquals(RaftRole.FOLLOWER, election.role(),
                "tryAcquireLease should step down and NOT re-acquire on monotonic expiry");
            assertFalse(k8sApiCalled.get(),
                "should not call K8s API after monotonic expiry step-down");
        } finally {
            election.stop();
        }
    }

    @Test
    void onAppendEntriesAfterStopShouldBeNoOp() throws Exception {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
        ApiClient dummyClient = new ApiClient();
        K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);

        // Force to LEADER with a known epoch
        Field roleField = K8sLeaseElection.class.getDeclaredField("role");
        roleField.setAccessible(true);
        roleField.set(election, RaftRole.LEADER);

        Field epochField = K8sLeaseElection.class.getDeclaredField("currentEpoch");
        epochField.setAccessible(true);
        epochField.set(election, 1L);

        AtomicBoolean followerFired = new AtomicBoolean(false);
        election.addBecomeFollowerListener(epoch -> followerFired.set(true));

        // Stop the election — sets role=FOLLOWER, stopped=true
        election.stop();

        // Try to step down via AppendEntries with a higher epoch — should be no-op
        // because stopped=true. Without the guard, onAppendEntries would update
        // currentEpoch to 5 and currentLeader to "other-pod" even after stop.
        election.onAppendEntries(5L, "other-pod");

        assertEquals(RaftRole.FOLLOWER, election.role(),
            "role should be FOLLOWER (set by stop()), not changed by onAppendEntries");
        assertFalse(followerFired.get(),
            "onBecomeFollower callback should NOT fire after stop");
        assertEquals(1L, election.currentEpoch(),
            "epoch should NOT be updated after stop");
        assertEquals(null, election.currentLeader(),
            "leader should NOT be updated after stop");
    }

    @Test
    void multipleListenersShouldAllFire() throws Exception {
        K8sLeaseConfig config = new K8sLeaseConfig(15, 4, "default", "loomq-leader", "pod-1");
        ApiClient dummyClient = new ApiClient();
        K8sLeaseElection election = new K8sLeaseElection(config, wal, dummyClient);
        try {
            AtomicBoolean leader1Fired = new AtomicBoolean(false);
            AtomicBoolean leader2Fired = new AtomicBoolean(false);
            AtomicBoolean follower1Fired = new AtomicBoolean(false);
            AtomicBoolean follower2Fired = new AtomicBoolean(false);

            election.addBecomeLeaderListener(epoch -> leader1Fired.set(true));
            election.addBecomeLeaderListener(epoch -> leader2Fired.set(true));
            election.addBecomeFollowerListener(epoch -> follower1Fired.set(true));
            election.addBecomeFollowerListener(epoch -> follower2Fired.set(true));

            // Force to LEADER — should fire both leader listeners
            Method becomeLeader =
                K8sLeaseElection.class.getDeclaredMethod("becomeLeader", long.class);
            becomeLeader.setAccessible(true);
            becomeLeader.invoke(election, 1L);

            assertTrue(leader1Fired.get(), "first leader listener should fire");
            assertTrue(leader2Fired.get(), "second leader listener should fire");

            // Force to FOLLOWER — should fire both follower listeners
            Method becomeFollower =
                K8sLeaseElection.class.getDeclaredMethod("becomeFollower", String.class, long.class);
            becomeFollower.setAccessible(true);
            becomeFollower.invoke(election, "other-pod", 2L);

            assertTrue(follower1Fired.get(), "first follower listener should fire");
            assertTrue(follower2Fired.get(), "second follower listener should fire");
        } finally {
            election.stop();
        }
    }

}
