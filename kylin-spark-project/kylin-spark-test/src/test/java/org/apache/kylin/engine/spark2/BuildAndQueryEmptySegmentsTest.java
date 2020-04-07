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
package org.apache.kylin.engine.spark2;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.TableDesc;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.JavaConversions;

public class BuildAndQueryEmptySegmentsTest extends LocalWithSparkSessionTest {

    private static final String CUBE_NAME1 = "ci_inner_join_cube";

    private static final String SQL = "select\n" + " count(1) as TRANS_CNT \n" + " from test_kylin_fact \n"
            + " group by ORDER_ID";

    private static final String SQL_DERIVED = "SELECT \n" + "test_cal_dt.WEEK_BEG_DT\n"
            + "FROM test_kylin_fact inner JOIN edw.test_cal_dt as test_cal_dt \n"
            + "ON test_kylin_fact.cal_dt=test_cal_dt.cal_dt \n"
            + "WHERE test_kylin_fact.cal_dt>'2009-06-01' and test_kylin_fact.cal_dt<'2013-01-01' \n"
            + "GROUP BY test_cal_dt.WEEK_BEG_DT";

    @Before
    public void init() throws Exception {
        super.init();
        CubeInstance cube = cubeMgr.getCube(CUBE_NAME1);
        TableDesc factTable = MetadataConverter.extractFactTable(cube);
        TableDesc lookupTable = null;

        for (TableDesc tableDesc: JavaConversions.asJavaCollection(MetadataConverter.extractLookupTable(cube))) {
            if (tableDesc.tableName().equalsIgnoreCase("test_cal_dt")) {
                lookupTable = tableDesc;
                break;
            }
        }
        KylinSparkEnv.getSparkSession().read().schema(factTable.toSchema()).csv("../../examples/test_case_data/parquet_test/data/DEFAULT.TEST_KYLIN_FACT.csv")
                .createOrReplaceTempView("TEST_KYLIN_FACT");
        KylinSparkEnv.getSparkSession().read().schema(lookupTable.toSchema()).csv("../../examples/test_case_data/parquet_test/data/EDW.TEST_CAL_DT.csv")
                .createOrReplaceTempView("TEST_CAL_DT");
    }

    @After
    public void cleanup() {
        DefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    @Ignore("Ignore with the introduce of Parquet storage")
    public void testEmptySegments() throws Exception {
        cleanupSegments(CUBE_NAME1);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        buildCube(CUBE_NAME1, f.parse("2009-01-01").getTime(), f.parse("2009-06-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(0).getInputRecords());

        testQueryUnequal(SQL);
        testQueryUnequal(SQL_DERIVED);

        buildCube(CUBE_NAME1, f.parse("2009-06-01").getTime(), f.parse("2010-01-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(1).getInputRecords());
        buildCube(CUBE_NAME1, f.parse("2010-01-01").getTime(), f.parse("2011-01-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(2).getInputRecords());
        buildCube(CUBE_NAME1, f.parse("2011-01-01").getTime(), f.parse("2015-01-01").getTime());
        Assert.assertNotEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(3).getInputRecords());

        mergeSegments(CUBE_NAME1, f.parse("2009-01-01").getTime(), f.parse("2010-01-01").getTime(), true);
        mergeSegments(CUBE_NAME1, f.parse("2010-01-01").getTime(), f.parse("2015-01-01").getTime(), true);

        testQuery(SQL);
        testQuery(SQL_DERIVED);
    }

    private void buildCube(String cubeName, long start, long end) throws Exception {
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));
    }

    private void testQuery(String sqlStr) {
        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertNotEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = NExecAndComp.querySparkSql(sql);
        Assert.assertEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private void testQueryUnequal(String sqlStr) {

        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = NExecAndComp.querySparkSql(sql);
        Assert.assertNotEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}
