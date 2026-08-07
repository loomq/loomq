package com.loomq.common;

import static org.junit.jupiter.api.Assertions.*;

import com.loomq.LoomqEngine;
import com.loomq.LoomqEngineFactory;
import com.loomq.domain.intent.AckMode;
import com.loomq.domain.intent.Intent;
import com.loomq.domain.intent.PrecisionTier;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FactoryFromPropertiesTest {
    @TempDir Path tmp;

    /**
     * A4:LoomqEngineFactory.createFromProperties 必须读 wheel.* 属性。用 wheel.slots_per_bucket=2,
     * 3 个同执行时刻 Intent 落同一桶,第 3 个溢出(默认 1024 不会)。证明 wheel.* 被读取。
     */
    @Test
    void fromPropertiesReadsWheelProps() throws Exception {
        Properties props = new Properties();
        props.setProperty("loomq.dataDir", tmp.toString());
        props.setProperty("wheel.slots_per_bucket", "2");

        LoomqEngine engine = LoomqEngineFactory.createFromProperties(props);
        engine.start();
        try {
            Instant exec = Instant.now().plusSeconds(10);
            for (int i = 0; i < 2; i++) {
                Intent it = new Intent("intent_fct0000000" + i);
                it.setExecuteAt(exec);
                it.setPrecisionTier(PrecisionTier.STANDARD);
                engine.createIntent(it, AckMode.ASYNC).join();
            }
            Intent third = new Intent("intent_fct00000002");
            third.setExecuteAt(exec);
            third.setPrecisionTier(PrecisionTier.STANDARD);
            assertThrows(RuntimeException.class,
                () -> engine.createIntent(third, AckMode.ASYNC).join(),
                "wheel.slots_per_bucket=2 from properties must cause bucket overflow");
        } finally {
            engine.close();
        }
    }

    /** A4:loomq.walDir(旧名)仍可作为 dataDir 回退。 */
    @Test
    void fromPropertiesLegacyWalDirFallback() throws Exception {
        Properties props = new Properties();
        props.setProperty("loomq.walDir", tmp.toString());
        LoomqEngine engine = LoomqEngineFactory.createFromProperties(props);
        engine.start();
        engine.close();
        // 能启停即通过
    }
}
