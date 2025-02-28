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

import java.io.File;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.udaf.BitmapSerAndDeSer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class NBitmapFunctionTest extends NLocalWithSparkSessionTest {

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        JobContextUtil.getJobContext(getTestConfig());

        populateSSWithCSVData(getTestConfig(), getProject(), ss);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        cleanupTestMetadata();
        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "intersect_count";
    }

    @Test
    public void testBitmapFunction() throws Exception {
        fullBuild("741ca86a-1f13-46da-a59f-95fb68615e3b");
        fullBuild("741ca86a-1f13-46da-a59f-95fb68615e3z");

        testDateType();

        testMultiMeasures();

        testCommomCase1();

        testCommomCase2();

        testWithUnion();

        testWithLimit();

        testIntersectCountByCol();

        testIntersectCountByColMultiRows();

        testIntersectCount();

        testIntersectValue();

        testExplodeIntersectValue();

        testHllcCanNotAnswerBitmapUUID();

        testSubtractBitmapValue();

        testSubtractBitmapUUID();

        testBitmapBuild();

        testIntersectBimapUuidFunc();

        testUnionBimapUuidFunc();

        testBimapUuidToArrayFunc();
    }

    private void testBitmapBuild() throws SQLException {
        List<String> result;

        //================= constant case
        String query1 = "select bitmap_build(1)";
        result = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("AAAAAAEAAAAAOjAAAAEAAAAAAAAAEAAAAAEA", result.get(0));

        //================= normal case
        String query2 = "select CAL_DT, bitmap_build(TEST_COUNT_DISTINCT_BITMAP) as first_day "
                + "from test_kylin_fact where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by CAL_DT order by CAL_DT";
        result = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-01-01,AAAAAAEAAAAAOzAAAAEAAA0AAQABAA0A", result.get(0));
        Assert.assertEquals("2012-01-02,AAAAAAEAAAAAOzAAAAEAAAkAAgAFAAAADwAIAA==", result.get(1));
        Assert.assertEquals("2012-01-03,AAAAAAEAAAAAOjAAAAEAAAAAAAQAEAAAABMAGAAZABoAGwA=", result.get(2));

        //================= pushdown case
        String query3 = "select CAL_DT, bitmap_build(LEAF_CATEG_ID) from test_kylin_fact "
                + "where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') group by CAL_DT "
                + "order by CAL_DT";
        result = ExecAndComp.querySparkSql(query3).collectAsList().stream().map(row -> row.toSeq().mkString(","))
                .collect(Collectors.toList());
        Assert.assertEquals(
                "2012-01-01,AAAAAAEAAAAAOjAAAAMAAAAAAAUAAQABAAIAAwAgAAAALAAAADAAAADDA0UFIi0FUPKK4/LFc7h1yiVkQ05shq4=",
                result.get(0));
        Assert.assertEquals("2012-01-02,AAAAAAEAAAAAOjAAAAIAAAAAAAYAAQACABgAAAAmAAAATQVKJ31ABVDdX3uckfmRJ7h1CpM=",
                result.get(1));
        Assert.assertEquals("2012-01-03,AAAAAAEAAAAAOjAAAAMAAAAAAAEAAQAAAAIAAQAgAAAAJAAAACYAAADSJIFRkSdaXuWn",
                result.get(2));
    }

    private void testDateType() throws SQLException {
        String query = "select CAL_DT, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday "
                + "from test_kylin_fact " + "where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by CAL_DT " + "order by CAL_DT ";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-01-01,14,0,0,0,0", result.get(0));
        Assert.assertEquals("2012-01-02,0,10,0,0,0", result.get(1));
        Assert.assertEquals("2012-01-03,0,0,5,0,0", result.get(2));
    }

    private void testMultiMeasures() throws SQLException {
        String query = "select week_beg_dt as week, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC']) as a, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Auction']) as b, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['Others']) as c, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction']) as ab, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Others']) as ac, "
                + "intersect_count( TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC', 'Auction', 'Others']) as abc, "
                + "count(distinct TEST_COUNT_DISTINCT_BITMAP) as sellers, count(*) as cnt "
                + "from test_kylin_fact left join edw.test_cal_dt on test_kylin_fact.cal_dt = edw.test_cal_dt.CAL_DT "
                + "where week_beg_dt in (DATE '2013-12-22', DATE '2012-06-23') group by week_beg_dt order by week_beg_dt";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-06-23,21,17,13,0,0,0,90,94", result.get(0));
        Assert.assertEquals("2013-12-22,18,22,13,0,0,0,98,99", result.get(1));
    }

    private void testCommomCase1() throws SQLException {
        String query = "select LSTG_FORMAT_NAME, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday "
                + "from test_kylin_fact where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by LSTG_FORMAT_NAME order by LSTG_FORMAT_NAME";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("ABIN,6,4,2,0,0", result.get(0));
        Assert.assertEquals("Auction,4,3,1,0,0", result.get(1));
        Assert.assertEquals("FP-GTC,2,2,0,0,0", result.get(2));
        Assert.assertEquals("FP-non GTC,2,1,0,0,0", result.get(3));
        Assert.assertEquals("Others,0,0,2,0,0", result.get(4));
    }

    private void testCommomCase2() throws SQLException {
        String query4 = "select LEAF_CATEG_ID, "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day "
                + "from test_kylin_fact where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03') "
                + "group by LEAF_CATEG_ID " + "order by LEAF_CATEG_ID";
        List<String> result = ExecAndComp.queryModel(getProject(), query4).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("963,1", result.get(0));
        Assert.assertEquals("1349,1", result.get(1));
        Assert.assertEquals("11554,1", result.get(2));
        Assert.assertEquals("20485,1", result.get(3));
        Assert.assertEquals("35570,1", result.get(4));
        Assert.assertEquals("62179,2", result.get(5));
        Assert.assertEquals("95173,1", result.get(6));
        Assert.assertEquals("95672,2", result.get(7));
        Assert.assertEquals("140746,1", result.get(8));
        Assert.assertEquals("148324,1", result.get(9));
        Assert.assertEquals("158798,1", result.get(10));
        Assert.assertEquals("175750,1", result.get(11));
    }

    private void testWithUnion() throws SQLException {
        String query = "SELECT (SELECT '2012-01-01') AS sdate, "
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-01']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-02']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-01',date'2012-01-03'])"
                + "FROM   test_kylin_fact WHERE cal_dt >= date '2012-01-01' AND cal_dt <  date'2012-01-07' "
                + "UNION ALL " + "SELECT (SELECT '2012-01-02') AS sdate, "
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-02']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-03']),"
                + "       intersect_count(TEST_COUNT_DISTINCT_BITMAP, cal_dt, array[date'2012-01-02',date'2012-01-04'])"
                + "FROM   test_kylin_fact WHERE  cal_dt >= date '2012-01-02' AND cal_dt < date'2012-01-07'"
                + "order by sdate";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("2012-01-01,14,1,0", result.get(0));
        Assert.assertEquals("2012-01-02,10,1,0", result.get(1));
    }

    private void testWithLimit() throws SQLException {
        String query = "select intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day "
                + "from test_kylin_fact " + "limit 1";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("14", result.get(0));
    }

    private void testIntersectCountByColMultiRows() throws SQLException {
        String query1 = "select intersect_count_by_col(Array[t1.a1]), LSTG_FORMAT_NAME from "
                + "    (select bitmap_uuid(SELLER_ID) as a1, LSTG_FORMAT_NAME "
                + "        from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME) t1 order by LSTG_FORMAT_NAME";

        List<String> result1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("855,ABIN", result1.get(0));
        Assert.assertEquals("896,Auction", result1.get(1));
        Assert.assertEquals("858,FP-GTC", result1.get(2));
        Assert.assertEquals("870,FP-non GTC", result1.get(3));
        Assert.assertEquals("855,Others", result1.get(4));
    }

    private void testIntersectCountByCol() throws Exception {
        String query1 = "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others']) as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-.*GTC', 'Others'], 'REGEXP') as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(SELLER_ID) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        SELLER_ID, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as a2 " + "from TEST_KYLIN_FACT) t2";

        List<String> result1 = ExecAndComp.queryModel(getProject(), query1).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("841", result1.get(0));
        Assert.assertEquals("841", result1.get(1));
        Assert.assertEquals("841", result1.get(2));

        String query2 = "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others']) as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-.*GTC', 'Others'], 'REGEXP') as a2 " + "from TEST_KYLIN_FACT) t2 " + "union all "
                + "select intersect_count_by_col(Array[t1.a1,t2.a2]) from "
                + "    (select bitmap_uuid(TEST_COUNT_DISTINCT_BITMAP) as a1 " + "        from TEST_KYLIN_FACT) t1, "
                + "    (select intersect_bitmap_uuid_v2( " + "        TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, "
                + "        array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as a2 " + "from TEST_KYLIN_FACT) t2";
        List<String> result2 = ExecAndComp.queryModel(getProject(), query2).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("862", result2.get(0));
        Assert.assertEquals("862", result2.get(1));
        Assert.assertEquals("862", result2.get(2));
    }

    private void testIntersectCount() throws SQLException {
        String query = "select "
                + "intersect_count(TEST_COUNT_DISTINCT_BITMAP, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others']) as a, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_count_v2(TEST_COUNT_DISTINCT_BITMAP, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("862,862,862", result.get(0));
    }

    private void testIntersectValue() throws SQLException {
        String query = "select "
                + "intersect_value(LSTG_SITE_ID, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others']) as a, "
                + "intersect_value_v2(LSTG_SITE_ID, LSTG_FORMAT_NAME, array['FP-.*GTC', 'Others'], 'REGEXP') as b, "
                + "intersect_value_v2(LSTG_SITE_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING') as c "
                + "from test_kylin_fact ";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("WrappedArray(0, 2, 3, 15, 23, 100, 101, 211),"
                + "WrappedArray(0, 2, 3, 15, 23, 100, 101, 211)," + "WrappedArray(0, 2, 3, 15, 23, 100, 101, 211)",
                result.get(0));
    }

    private void testExplodeIntersectValue() throws SQLException {
        String query = "select "
                + "explode(intersect_value(LSTG_SITE_ID, lstg_format_name, array['FP-GTC|FP-non GTC', 'Others'])) as a "
                + "from test_kylin_fact ";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("0", result.get(0));
        Assert.assertEquals("2", result.get(1));
        Assert.assertEquals("3", result.get(2));
        Assert.assertEquals("15", result.get(3));
        Assert.assertEquals("23", result.get(4));
        Assert.assertEquals("100", result.get(5));
        Assert.assertEquals("101", result.get(6));
        Assert.assertEquals("211", result.get(7));
    }

    private void testHllcCanNotAnswerBitmapUUID() throws SQLException {
        String query = "select intersect_count_by_col(Array[t1.a1]), LSTG_FORMAT_NAME from"
                + " (select bitmap_uuid(SELLER_ID) as a1, LSTG_FORMAT_NAME from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME) t1"
                + " order by LSTG_FORMAT_NAME";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("855,ABIN", result.get(0));
        Assert.assertEquals("896,Auction", result.get(1));
        Assert.assertEquals("858,FP-GTC", result.get(2));
        Assert.assertEquals("870,FP-non GTC", result.get(3));
        Assert.assertEquals("855,Others", result.get(4));
    }

    private void testSubtractBitmapValue() throws SQLException {
        String query = "select subtract_bitmap_value("
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING'),"
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN', 'Auction'], 'RAWSTRING'))"
                + "from TEST_KYLIN_FACT";
        List<Integer> acutal = ExecAndComp.queryModel(getProject(), query).collectAsList().get(0).getList(0).stream()
                .map(row -> Integer.parseInt(row.toString())).collect(Collectors.toList());

        Dataset<Row> fg = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'FP-GTC'");
        Dataset<Row> fng = ss
                .sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'FP-non GTC'");
        Dataset<Row> ot = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'Others'");
        Dataset<Row> ab = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'ABIN'");
        Dataset<Row> au = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'Auction'");
        List<Integer> expect = fg.union(fng).intersect(ot).except(ab.intersect(au)).sort(new Column("SELLER_ID"))
                .collectAsList().stream().map(row -> row.getInt(0)).collect(Collectors.toList());
        Assert.assertEquals(expect.size(), acutal.size());
        for (int i = 0; i < acutal.size(); i++) {
            Assert.assertEquals(expect.get(i), acutal.get(i));
        }
    }

    private void testSubtractBitmapUUID() throws SQLException {
        String query = "select intersect_count_by_col(Array[t1.a1, t2.a2]) from (select subtract_bitmap_uuid("
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC|FP-non GTC', 'Others'], 'RAWSTRING'),"
                + "intersect_bitmap_uuid_v2(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN', 'Auction'], 'RAWSTRING')) as a1 "
                + "from TEST_KYLIN_FACT) t1, (select bitmap_uuid(SELLER_ID) as a2 from TEST_KYLIN_FACT) t2";
        List<String> result = ExecAndComp.queryModel(getProject(), query).collectAsList().stream()
                .map(row -> row.toSeq().mkString(",")).collect(Collectors.toList());
        Assert.assertEquals("210", result.get(0));
    }

    private void testIntersectBimapUuidFunc() throws Exception {
        Dataset<Row> expect = ss
                .sql("select LSTG_FORMAT_NAME,collect_set( SELLER_ID) from TEST_KYLIN_FACT group by LSTG_FORMAT_NAME");

        List<Row> rows = expect.collectAsList();
        List<Object> list0 = rows.get(0).getList(1);
        List<Object> list1 = rows.get(1).getList(1);
        List<Object> list2 = rows.get(2).getList(1);
        List<Object> list3 = rows.get(3).getList(1);
        List<Object> list4 = rows.get(4).getList(1);
        List intersection = ListUtils.intersection(list0, list1);
        intersection = ListUtils.intersection(intersection, list2);
        intersection = ListUtils.intersection(intersection, list3);
        intersection = ListUtils.intersection(intersection, list4);
        Collections.sort(intersection);

        String countSql = "select intersect_bitmap_uuid_count(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";

        int countResult = ExecAndComp.queryModel(getProject(), countSql).collectAsList().get(0).getInt(0);
        Assert.assertEquals(intersection.size(), countResult);

        String valueSql = "select intersect_bitmap_uuid_value_all(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";

        List<Object> valueResult = ExecAndComp.queryModel(getProject(), valueSql).collectAsList().get(0).getList(0);
        for (int i = 0; i < intersection.size(); i++) {
            Assert.assertEquals(intersection.get(i).toString(), valueResult.get(i).toString());
        }

        String valueTmp = "select intersect_bitmap_uuid_value(uuid,limit,offset) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT ) t1";

        int limit = 100;
        int offset = 0;
        int pageSize = intersection.size() / limit + 1;
        for (int page = 1; page <= pageSize; page++) {
            offset = (page - 1) * limit;
            valueSql = valueTmp.replace("limit", limit + "").replace("offset", offset + "");
            valueResult = ExecAndComp.queryModel(getProject(), valueSql).collectAsList().get(0).getList(0);
            for (int i = 0; i < valueResult.size(); i++) {
                Assert.assertEquals(intersection.get(offset).toString(), valueResult.get(i).toString());
                offset += 1;
            }
        }

        String ep = "both limit and offset must be >= 0";
        // test limit < 0
        final String valueSql2 = valueTmp.replace("limit", -1 + "").replace("offset", offset + "");
        Assert.assertThrows(ep, SQLException.class,
                () -> ExecAndComp.queryModel(getProject(), valueSql2).collectAsList().get(0).getList(0));

        // test offset < 0
        final String valueSql3 = valueTmp.replace("limit", 100 + "").replace("offset", -1 + "");
        Assert.assertThrows(ep, SQLException.class,
                () -> ExecAndComp.queryModel(getProject(), valueSql3).collectAsList().get(0).getList(0));

        String distinctSql = "select intersect_bitmap_uuid_distinct(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT ) t1";
        byte[] distinctResult = (byte[]) ExecAndComp.queryModel(getProject(), distinctSql).collectAsList().get(0)
                .get(0);
        Roaring64NavigableMap bitmap = BitmapSerAndDeSer.get().deserialize(distinctResult);
        int i = 0;
        Iterator<Long> iterator = bitmap.iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            Assert.assertEquals(next.toString(), intersection.get(i).toString());
            i++;
        }
    }

    private void testUnionBimapUuidFunc() throws Exception {
        Dataset<Row> totalIdQuery = ss.sql("select distinct SELLER_ID from TEST_KYLIN_FACT order by SELLER_ID asc");

        List<Integer> tatalIds = totalIdQuery.collectAsList().stream().map(row -> row.getInt(0))
                .collect(Collectors.toList());

        String countSql = "select union_bitmap_uuid_count(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";

        int countResult = ExecAndComp.queryModel(getProject(), countSql).collectAsList().get(0).getInt(0);
        Assert.assertEquals(tatalIds.size(), countResult);

        String valueSql = "select union_bitmap_uuid_value_all(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT ) t1";

        List<Object> valueResult = ExecAndComp.queryModel(getProject(), valueSql).collectAsList().get(0).getList(0);
        for (int i = 0; i < tatalIds.size(); i++) {
            Assert.assertEquals(tatalIds.get(i).toString(), valueResult.get(i).toString());
        }

        String valueTmp = "select union_bitmap_uuid_value(uuid,limit,offset) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT ) t1";

        int limit = 100;
        int offset = 0;
        int pageSize = tatalIds.size() / limit + 1;
        for (int page = 1; page <= pageSize; page++) {
            offset = (page - 1) * limit;
            valueSql = valueTmp.replace("limit", limit + "").replace("offset", offset + "");
            valueResult = ExecAndComp.queryModel(getProject(), valueSql).collectAsList().get(0).getList(0);
            for (int i = 0; i < valueResult.size(); i++) {
                Assert.assertEquals(tatalIds.get(offset).toString(), valueResult.get(i).toString());
                offset += 1;
            }
        }

        String distinctSql = "select union_bitmap_uuid_distinct(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['FP-non GTC']) as uuid "
                + "from TEST_KYLIN_FACT UNION "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";
        byte[] distinctResult = (byte[]) ExecAndComp.queryModel(getProject(), distinctSql).collectAsList().get(0)
                .get(0);
        Roaring64NavigableMap bitmap = BitmapSerAndDeSer.get().deserialize(distinctResult);
        int i = 0;
        Iterator<Long> iterator = bitmap.iterator();
        while (iterator.hasNext()) {
            Long next = iterator.next();
            Assert.assertEquals(next.toString(), tatalIds.get(i).toString());
            i++;
        }
    }

    private void testBimapUuidToArrayFunc() throws Exception {
        String arraySql = "select bitmap_uuid_to_array(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";
        List<Object> ArrayResult = ExecAndComp.queryModel(getProject(), arraySql).collectAsList().get(0).getList(0);

        String valueSql = "select intersect_bitmap_uuid_value_all(uuid) uuid from ( "
                + "select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Others']) as uuid "
                + "from TEST_KYLIN_FACT) t1";
        List<Object> valueResult = ExecAndComp.queryModel(getProject(), valueSql).collectAsList().get(0).getList(0);
        for (int i = 0; i < ArrayResult.size(); i++) {
            Assert.assertEquals(ArrayResult.get(i).toString(), valueResult.get(i).toString());
        }
    }
}
