package com.kylinolap.query.test;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.metadata.realization.RealizationRegistry;
import com.kylinolap.metadata.realization.RealizationType;

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
