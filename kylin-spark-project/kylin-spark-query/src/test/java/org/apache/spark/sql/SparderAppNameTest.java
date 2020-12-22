/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparderAppNameTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(SparderAppNameTest.class);

    @BeforeClass
    public static void beforeClass() {
    }

    @Override
    @Before
    public void setup() throws SchedulerException {
        super.setup();
        if (Shell.MAC)
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        conf.setProperty("kylin.query.spark-conf.spark.master", "local");
        SparderContext.getOriginalSparkSession();
    }

    @After
    public void after() {
        SparderContext.stopSpark();
        super.after();
    }

    @Test
    public void testThreadSparkSession() {
        Assert.assertTrue(StringUtils.isNotBlank(
                SparderContext.getOriginalSparkSession().sparkContext().getConf()
                        .get("spark.app.name")));
        Assert.assertTrue(SparderContext.getOriginalSparkSession().sparkContext().getConf()
                .get("spark.app.name").equals("sparder_on_localhost-7070"));

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.sparder-context.app-name", "test-sparder-app-name");
        SparderContext.restartSpark();

        Assert.assertTrue(StringUtils.isNotBlank(
                SparderContext.getOriginalSparkSession().sparkContext().getConf()
                        .get("spark.app.name")));
        Assert.assertTrue(SparderContext.getOriginalSparkSession().sparkContext().getConf()
                .get("spark.app.name").equals("test-sparder-app-name"));
        config.setProperty("kylin.query.sparder-context.app-name", "");
    }
}
