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

package org.apache.kylin.query.monitor;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.spark.sql.SparderContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparderContextCanaryTest extends LocalWithSparkSessionTest {
    @Override
    @Before
    public void setup() throws SchedulerException {
        super.setup();
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        // the default value of kylin.query.spark-conf.spark.master is yarn,
        // which will read from kylin-defaults.properties
        conf.setProperty("kylin.query.spark-conf.spark.master", "local");
        // create a new SparkSession of Sparder
        SparderContext.initSpark();
    }

    @After
    public void after() {
        SparderContext.stopSpark();
        KylinConfig.getInstanceFromEnv()
                .setProperty("kylin.query.spark-conf.spark.master", "yarn");
        super.after();
    }

    @Test
    public void testSparderKilled() {
        // first check should be good
        Boolean ss = SparderContext.isSparkAvailable();
        Assert.assertTrue(SparderContext.isSparkAvailable());

        // stop sparder and check again, the sparder context should auto-restart
        SparderContext.getOriginalSparkSession().stop();
        Assert.assertFalse(SparderContext.isSparkAvailable());

        SparderContextCanary.monitor();

        Assert.assertTrue(SparderContext.isSparkAvailable());

        SparderContextCanary.monitor();
        Assert.assertEquals(0, SparderContextCanary.getErrorAccumulated());
    }

    @Test
    public void testSparderTimeout() {
        // first check should be GOOD
        Assert.assertTrue(SparderContext.isSparkAvailable());

        // set kylin.canary.sqlcontext-error-response-ms to 1
        // And SparkContextCanary numberCount will timeout
        Assert.assertEquals(0, SparderContextCanary.getErrorAccumulated());
        System.setProperty("kylin.canary.sparder-context-error-response-ms", "1");
        SparderContextCanary.monitor();

        // errorAccumulated increase
        Assert.assertEquals(1, SparderContextCanary.getErrorAccumulated());

        // reach threshold to restart spark. Reset errorAccumulated.
        SparderContextCanary.monitor();
        Assert.assertEquals(2, SparderContextCanary.getErrorAccumulated());
        SparderContextCanary.monitor();
        Assert.assertEquals(3, SparderContextCanary.getErrorAccumulated());

        Assert.assertTrue(SparderContext.isSparkAvailable());

        System.clearProperty("kylin.canary.sparder-context-error-response-ms");

    }

    public void createTestMetadata() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }
}
