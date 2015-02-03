package org.apache.kylin.query.test;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;

/**
 * Created by qianzhou on 1/26/15.
 */
public class RealizationRegistryTest extends KylinQueryTest {

    @Test
    public void test() throws Exception {
        final RealizationRegistry registry = RealizationRegistry.getInstance(KylinConfig.getInstanceFromEnv());
        final Set<RealizationType> realizationTypes = registry.getRealizationTypes();
        assertEquals(RealizationType.values().length, realizationTypes.size());
        for (RealizationType type: RealizationType.values()) {
            assertTrue(realizationTypes.contains(type));
        }
    }
}
