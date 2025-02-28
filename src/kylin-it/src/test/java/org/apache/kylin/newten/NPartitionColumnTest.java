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

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.TimeZoneTestRunner;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import lombok.val;

@RunWith(TimeZoneTestRunner.class)
public class NPartitionColumnTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.createTestMetadata("src/test/resources/ut_meta/partition_col");

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());
    }

    @Override
    protected String[] getOverlay() {
        return new String[] { "src/test/resources/ut_meta/partition_col" };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
    }

    @Test
    public void testVariousPartitionCol() throws Exception {
        // build three segs
        // [2009-01-01 00:00:00, 2011-01-01 00:00:00)
        // [2011-01-01 00:00:00, 2013-01-01 00:00:00)
        // [2013-01-01 00:00:00, 2015-01-01 00:00:00)
        List<String> dfs = new ArrayList<>();
        dfs.add("INT_PAR_COL");
        dfs.add("LONG_PAR_COL");
        for (int i = 1; i < 7; i++) {
            dfs.add("STR_PAR_COL" + i);
        }

        for (String df : dfs) {
            buildMultiSegs(df);
        }

        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());

        val base = "select count(*) from TEST_PAR_COL ";

        val sql = new ArrayList<String>();
        sql.add(base + "where INT_PAR_COL >= 20090101 and INT_PAR_COL < 20110101");
        sql.add(base + "where INT_PAR_COL >= 20110101 and INT_PAR_COL < 20130101");
        sql.add(base + "where INT_PAR_COL >= 20130101 and INT_PAR_COL < 20150101");

        sql.add(base + "where LONG_PAR_COL >= 20090101 and LONG_PAR_COL < 20110101");
        sql.add(base + "where LONG_PAR_COL >= 20110101 and LONG_PAR_COL < 20130101");
        sql.add(base + "where LONG_PAR_COL >= 20130101 and LONG_PAR_COL < 20150101");

        sql.add(base + "where STR_PAR_COL1 >= '20090101' and STR_PAR_COL1 < '20110101'");
        sql.add(base + "where STR_PAR_COL1 >= '20110101' and STR_PAR_COL1 < '20130101'");
        sql.add(base + "where STR_PAR_COL1 >= '20130101' and STR_PAR_COL1 < '20150101'");

        sql.add(base + "where STR_PAR_COL2 >= '2009-01-01' and STR_PAR_COL2 < '2011-01-01'");
        sql.add(base + "where STR_PAR_COL2 >= '2011-01-01' and STR_PAR_COL2 < '2013-01-01'");
        sql.add(base + "where STR_PAR_COL2 >= '2013-01-01' and STR_PAR_COL2 < '2015-01-01'");

        sql.add(base + "where STR_PAR_COL3 >= '2009/01/01' and STR_PAR_COL3 < '2011/01/01'");
        sql.add(base + "where STR_PAR_COL3 >= '2011/01/01' and STR_PAR_COL3 < '2013/01/01'");
        sql.add(base + "where STR_PAR_COL3 >= '2013/01/01' and STR_PAR_COL3 < '2015/01/01'");

        sql.add(base + "where STR_PAR_COL4 >= '2009.01.01' and STR_PAR_COL4 < '2011.01.01'");
        sql.add(base + "where STR_PAR_COL4 >= '2011.01.01' and STR_PAR_COL4 < '2013.01.01'");
        sql.add(base + "where STR_PAR_COL4 >= '2013.01.01' and STR_PAR_COL4 < '2015.01.01'");

        sql.add(base + "where STR_PAR_COL5 >= '2009-01-01 00:00:00' and STR_PAR_COL5 < '2011-01-01 00:00:00'");
        sql.add(base + "where STR_PAR_COL5 >= '2011-01-01 00:00:00' and STR_PAR_COL5 < '2013-01-01 00:00:00'");
        sql.add(base + "where STR_PAR_COL5 >= '2013-01-01 00:00:00' and STR_PAR_COL5 < '2015-01-01 00:00:00'");

        sql.add(base + "where STR_PAR_COL6 >= '2009-01-01 00:00:00.000' and STR_PAR_COL6 < '2011-01-01 00:00:00.000'");
        sql.add(base + "where STR_PAR_COL6 >= '2011-01-01 00:00:00.000' and STR_PAR_COL6 < '2013-01-01 00:00:00.000'");
        sql.add(base + "where STR_PAR_COL6 >= '2013-01-01 00:00:00.000' and STR_PAR_COL6 < '2015-01-01 00:00:00.000'");

        sql.add("select STR_PAR_COL6 from TEST_PAR_COL " + "where  STR_PAR_COL6 <  '1992-01-01 00:00:00.000'");
        sql.add(base + "where STR_PAR_COL4 < '1992.01.01' " + "union " + base + "where STR_PAR_COL4 < '1992.01.01'");

        ExecAndComp.execAndCompareQueryList(sql, getProject(), ExecAndComp.CompareLevel.SAME, "default");
    }

    @Override
    public String getProject() {
        return "partition_col";
    }
}
