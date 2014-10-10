package com.kylinolap.common.persistence;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.ClasspathUtil;

/**
 * Created by honma on 9/18/14.
 */
@Ignore
public class ResourceToolTest {
    @Before
    public void setup() throws Exception {
        ClasspathUtil.addClasspath(new File("../examples/test_case_data/hadoop-site").getAbsolutePath());
    }

    @Test
    public void test() throws IOException {
        ResourceTool.copy(KylinConfig.createInstanceFromUri("../examples/test_case_data"), KylinConfig.createInstanceFromUri("../examples/test_case_data/kylin.properties"));
    }

}
