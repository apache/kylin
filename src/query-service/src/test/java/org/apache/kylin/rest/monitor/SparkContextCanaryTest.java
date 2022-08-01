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
package org.apache.kylin.rest.monitor;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SparkContextCanaryTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
        overwriteSystemProp("spark.local", "true");
        SparderEnv.init();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
        SparderEnv.getSparkSession().stop();
    }

    @Test
    public void testSparkKilled() {
        // first check should be good
        Assert.assertTrue(SparderEnv.isSparkAvailable());

        // stop spark and check again, the spark context should auto-restart
        SparderEnv.getSparkSession().stop();
        Assert.assertFalse(SparderEnv.isSparkAvailable());

        SparkContextCanary.getInstance().monitor();

        Assert.assertTrue(SparderEnv.isSparkAvailable());

        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(0, SparkContextCanary.getInstance().getErrorAccumulated());
    }

    @Test
    public void testSparkTimeout() {
        // first check should be GOOD
        Assert.assertTrue(SparderEnv.isSparkAvailable());

        // set kylin.canary.sqlcontext-error-response-ms to 1
        // And SparkContextCanary numberCount will timeout
        Assert.assertEquals(0, SparkContextCanary.getInstance().getErrorAccumulated());
        overwriteSystemProp("kylin.canary.sqlcontext-error-response-ms", "1");
        SparkContextCanary.getInstance().monitor();

        // errorAccumulated increase
        Assert.assertEquals(1, SparkContextCanary.getInstance().getErrorAccumulated());

        // reach threshold to restart spark. Reset errorAccumulated.
        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(2, SparkContextCanary.getInstance().getErrorAccumulated());
        SparkContextCanary.getInstance().monitor();
        Assert.assertEquals(3, SparkContextCanary.getInstance().getErrorAccumulated());

        Assert.assertTrue(SparderEnv.isSparkAvailable());
        SparderEnv.getSparkSession().stop();
    }
}
