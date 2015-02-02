package com.kylinolap.metadata.tool;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HBaseMetadataTestCase;

public class HiveSourceTableLoaderTest extends HBaseMetadataTestCase {

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();
    }

    @Test
    public void test() throws IOException {
        if (!useSandbox())
            return;

        KylinConfig config = getTestConfig();
        String[] toLoad = new String[] { "DEFAULT.TEST_KYLIN_FACT", "EDW.TEST_CAL_DT" };
        Set<String> loaded = HiveSourceTableLoader.reloadHiveTables(toLoad, config);

        assertTrue(loaded.size() == toLoad.length);
        for (String str : toLoad)
            assertTrue(loaded.contains(str));
    }

}
