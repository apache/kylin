package com.kylinolap.metadata.realization;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.rest.service.ServiceTestBase;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 1/26/15.
 */
public class RealizationRegistryTest extends ServiceTestBase {


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
