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

package org.apache.kylin.query.engine;

import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.base.Charsets;
import org.apache.kylin.guava30.shaded.common.io.CharStreams;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.util.QueryUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryExecWithMetaTest extends NLocalWithSparkSessionTest {
    String project = "min_max";
    QueryExec queryExec;

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_meta/query_min_max_with_meta");
        overwriteSystemProp("kylin.query.using-metadata-answer-minmax-of-dimension", "true");
        queryExec = new QueryExec(project, getTestConfig());

    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testSingleTableModel() throws SQLException, IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.dropModel("4623e6d3-2ca2-319e-9a3f-e26bd819734f");
        String sql = getSql("/query/sql_min_max/query01.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        List<String> next = queryResult.getRowsIterable().iterator().next();
        Assert.assertEquals("1", next.get(0));
        Assert.assertEquals("60000", next.get(1));
        Assert.assertEquals("1.001", next.get(2));
    }

    @Test
    public void testSingleTableModelWithUnion() throws SQLException, IOException {
        String sql = getSql("/query/sql_min_max/query02.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(2, queryResult.getSize());
        Iterator<List<String>> iterator = queryResult.getRowsIterable().iterator();
        List<String> firstRow = iterator.next();
        Assert.assertEquals("1", firstRow.get(0));
        Assert.assertEquals("60000", firstRow.get(1));
        Assert.assertEquals("1.001", firstRow.get(2));
        List<String> secondRow = iterator.next();
        Assert.assertEquals("1", secondRow.get(0));
        Assert.assertEquals("60000", secondRow.get(1));
        Assert.assertEquals("1.001", secondRow.get(2));
    }

    @Test
    public void testSingleTableModelDoNotUseMetadata() throws IOException {
        overwriteSystemProp("kylin.query.using-metadata-answer-minmax-of-dimension", "false");
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.dropModel("4623e6d3-2ca2-319e-9a3f-e26bd819734f");
        String sql = getSql("/query/sql_min_max/query01.sql");
        try {
            queryExec.executeQuery(sql);
            Assert.fail();
        } catch (Exception e) {
            // because the index was not built when running this case
            Assert.assertTrue(e.getMessage().endsWith(
                    "parquet/c5eecd91-0b8a-f8d7-89c8-c0afdaa01800/d36576c3-8ca1-a567-e54d-5d4062d3b742/1 does not exist"));
        }
    }

    @Test
    public void testCanNotUseMetadataDueToGroupByNonConst() {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.dropModel("4623e6d3-2ca2-319e-9a3f-e26bd819734f");
        String sql = "select LO_ORDERKEY, min(LO_ORDERKEY) from ssb.lineorder group by LO_ORDERKEY";
        try {
            queryExec.executeQuery(sql);
            Assert.fail();
        } catch (Exception e) {
            // because the index was not built when running this case
            Assert.assertTrue(e.getMessage().endsWith(
                    "parquet/c5eecd91-0b8a-f8d7-89c8-c0afdaa01800/d36576c3-8ca1-a567-e54d-5d4062d3b742/1 does not exist"));
        }
    }

    @Test
    public void testSingleTableModelDoNotUseMetadataDueToExpressionAsParameter() throws IOException {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.dropModel("4623e6d3-2ca2-319e-9a3f-e26bd819734f");
        String sql = getSql("/query/sql_min_max/query07.sql");
        try {
            queryExec.executeQuery(sql);
            Assert.fail();
        } catch (Exception e) {
            // because the index was not built when running this case
            Assert.assertTrue(e.getMessage().endsWith(
                    "parquet/c5eecd91-0b8a-f8d7-89c8-c0afdaa01800/d36576c3-8ca1-a567-e54d-5d4062d3b742/1 does not exist"));
        }
    }

    @Test
    public void testCanNotUseMetadataDueToMinMaxColumnIsNotDimension() {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.dropModel("4623e6d3-2ca2-319e-9a3f-e26bd819734f");
        String sql = "select min(LO_ORDERPRIOTITY) from ssb.lineorder";
        try {
            queryExec.executeQuery(sql);
            Assert.fail();
        } catch (Exception e) {
            // because the index was not built when running this case
            Assert.assertTrue(e.getMessage().endsWith(
                    "parquet/c5eecd91-0b8a-f8d7-89c8-c0afdaa01800/d36576c3-8ca1-a567-e54d-5d4062d3b742/10001 does not exist"));
        }
    }

    @Test
    public void testCanNotUseMetadataDueToAggIsNotMinMax() {
        String sql = "SELECT SUM(LO_QUANTITY) FROM SSB.LINEORDER";
        try {
            queryExec.executeQuery(sql);
            Assert.fail();
        } catch (Exception e) {
            // because the index was not built when running this case
            Assert.assertTrue(e.getMessage().endsWith(
                    "parquet/c5eecd91-0b8a-f8d7-89c8-c0afdaa01800/d36576c3-8ca1-a567-e54d-5d4062d3b742/1 does not exist"));
        }
    }

    // has partition column
    @Test
    public void testInnerJoinModel() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query03.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        List<String> next = queryResult.getRowsIterable().iterator().next();
        Assert.assertEquals("1", next.get(0));
        Assert.assertEquals("60000", next.get(1));
        Assert.assertEquals("1.001", next.get(2));
    }

    @Test
    public void testInnerJoinModelWithIncrementalBuiltSegments() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query03-2.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        List<String> next = queryResult.getRowsIterable().iterator().next();
        Assert.assertEquals("1", next.get(0));
        Assert.assertEquals("20000", next.get(1));
        Assert.assertEquals("1.001", next.get(2));
    }

    @Test
    public void testInnerJoinModelWithSingleTableModel() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query04.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        List<String> next = queryResult.getRowsIterable().iterator().next();
        Assert.assertEquals("1", next.get(0));
        Assert.assertEquals("60000", next.get(1));
        Assert.assertEquals("1.001", next.get(2));
    }

    @Test
    public void testLeftJoinModel() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query05.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        List<String> next = queryResult.getRowsIterable().iterator().next();
        Assert.assertEquals("0", next.get(0));
        Assert.assertEquals("20", next.get(1));
        Assert.assertEquals("2023", next.get(2));
    }

    @Test
    public void testLeftJoinModelWithUnion() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query06.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(3, queryResult.getSize());
        Iterator<List<String>> iterator = queryResult.getRowsIterable().iterator();
        List<String> firstRow = iterator.next();
        Assert.assertEquals("0", firstRow.get(0));
        Assert.assertEquals("20", firstRow.get(1));
        Assert.assertEquals("2023", firstRow.get(2));
        List<String> secondRow = iterator.next();
        Assert.assertEquals("0", secondRow.get(0));
        Assert.assertEquals("20", secondRow.get(1));
        Assert.assertEquals("2023", secondRow.get(2));
        List<String> thirdRow = iterator.next();
        Assert.assertEquals("1", thirdRow.get(0));
        Assert.assertEquals("60000", thirdRow.get(1));
        Assert.assertEquals("1001", thirdRow.get(2));
    }

    @Test
    public void testAllType() throws IOException, SQLException {
        String sql = getSql("/query/sql_min_max/query08.sql");
        QueryResult queryResult = queryExec.executeQuery(sql);
        Assert.assertEquals(1, queryResult.getSize());
        Iterator<List<String>> iterator = queryResult.getRowsIterable().iterator();
        List<String> result = iterator.next();
        Assert.assertEquals("2147483648,21474836483289,2132,2147483647,-128,127,0,9,0.0,10000.0,"
                + "0.3255242,85208.3241,10.0000,201.3235,abc,xyz,aaaaaaaa,xxxxxxxxxxxxxxxxxxxxx,abcd,zzzz,"
                + "2001-01-01,2004-04-17,2004-04-01 00:00:00,2004-04-17 00:32:23.032,false,true,"
                + "null,null,null,null,null,null,null,null,null,null,null,null", String.join(",", result));
    }

    private String getSql(String path) throws IOException {
        String sql = CharStreams.toString(
                new InputStreamReader(Objects.requireNonNull(getClass().getResourceAsStream(path)), Charsets.UTF_8));
        return QueryUtil.removeCommentInSql(sql);
    }

}
