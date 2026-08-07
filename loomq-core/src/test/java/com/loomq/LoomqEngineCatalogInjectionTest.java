package com.loomq;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.domain.intent.PrecisionTier;
import com.loomq.domain.intent.PrecisionTierCatalog;
import com.loomq.domain.intent.PrecisionTierProfile;
import com.loomq.spi.DeliveryHandler;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LoomqEngineCatalogInjectionTest {

    private static final DeliveryHandler OK = i ->
        CompletableFuture.completedFuture(DeliveryHandler.DeliveryResult.SUCCESS);

    @Test void injectedCatalogControlsScheduler(@TempDir Path tmp) throws Exception {
        var base = PrecisionTierCatalog.defaultCatalog();
        EnumMap<PrecisionTier, PrecisionTierProfile> profiles = new EnumMap<>(PrecisionTier.class);
        for (PrecisionTier t : base.supportedTiers()) {
            PrecisionTierProfile p = base.profile(t);
            profiles.put(t, t == PrecisionTier.ULTRA ? p.withConsumerCount(8) : p);
        }
        PrecisionTierCatalog custom = PrecisionTierCatalog.of(profiles, PrecisionTier.STANDARD);

        try (LoomqEngine engine = LoomqEngine.builder()
                .dataDir(tmp).catalog(custom).deliveryHandler(OK).build()) {
            engine.start();
            PrecisionTierCatalog got = engine.getScheduler().getPrecisionTierCatalog();
            assertEquals(8, got.consumerCount(PrecisionTier.ULTRA));
            assertEquals(base.consumerCount(PrecisionTier.MILLI), got.consumerCount(PrecisionTier.MILLI));
        }
    }
}