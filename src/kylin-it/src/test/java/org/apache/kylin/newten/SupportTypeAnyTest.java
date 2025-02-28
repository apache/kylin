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


package org.apache.kylin.newten;

import java.sql.SQLException;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SupportTypeAnyTest extends NLocalWithSparkSessionTest {
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
    }

    @Override
    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws SQLException {
        String sql = "select replace(TEST_COUNT_DISTINCT_BITMAP, 'TEST', '') as HEADER from TEST_KYLIN_FACT where 1 = 0";
        Dataset<Row> dataset = ExecAndComp.queryModel(getProject(), sql);
        Assert.assertEquals("HEADER", dataset.schema().apply(0).name());
        Assert.assertEquals(0, dataset.collectAsList().size());
    }
}
