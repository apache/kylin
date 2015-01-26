package com.kylinolap.query.test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;
import com.kylinolap.metadata.realization.RealizationRegistry;
import com.kylinolap.metadata.realization.RealizationType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by qianzhou on 1/26/15.
 */
public class RealizationRegistryTest extends KylinQueryTest {

//    @Before
//    public void before() throws Exception {
//        ClasspathUtil.addClasspath(new File("../examples/test_case_data/sandbox/").getAbsolutePath());
//    }

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
