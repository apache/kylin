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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SubtractBitmapTest extends NLocalWithSparkSessionTest {

    private List subtract;

    @Override
    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        JobContextUtil.getJobContext(getTestConfig());

        populateSSWithCSVData(getTestConfig(), getProject(), ss);

        fullBuild("741ca86a-1f13-46da-a59f-95fb68615e3b");
        fullBuild("741ca86a-1f13-46da-a59f-95fb68615e3z");

        Dataset<Row> expect = ss.sql("select LSTG_FORMAT_NAME, collect_set(SELLER_ID) "
                + "from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'ABIN' or LSTG_FORMAT_NAME ='Auction' "
                + "group by LSTG_FORMAT_NAME order by LSTG_FORMAT_NAME asc");

        List<Row> rows = expect.collectAsList();
        List<Object> list0 = rows.get(0).getList(1);
        List<Object> list1 = rows.get(1).getList(1);
        subtract = ListUtils.subtract(list0, list1);
        Collections.sort(subtract);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
        FileUtils.deleteQuietly(new File("../kylin-it/metastore_db"));
    }

    @Override
    public String getProject() {
        return "intersect_count";
    }

    @Test
    public void testSubtractBimap() throws Exception {
        testSubtractBimapValueFunction();

        testSubtractBimapLimitOffsetValuesFunction();

        testSubtractBimapCountFunction();

        testSubtractBimapLimitOffsetValuesErrorFunction();
    }

    private void testSubtractBimapValueFunction() throws SQLException {
        Collection<Object[]> subtractBimapValues = testSubtractBimapValuesSql();
        for (Object[] object : subtractBimapValues) {
            String valueSql = (String) object[0];
            int rowsSize = (int) object[1];
            List<Row> rows = ExecAndComp.queryModel(getProject(), valueSql).collectAsList();
            Assert.assertEquals(rowsSize, rows.size());
            for (Row row : rows) {
                List<Object> valueResult = row.getList(0);
                Assert.assertTrue(CollectionUtils.isNotEmpty(valueResult));
                for (int i = 0; i < subtract.size(); i++) {
                    Assert.assertEquals(subtract.get(i).toString(), valueResult.get(i).toString());
                }
            }
        }
    }

    private Collection<Object[]> testSubtractBimapValuesSql() {
        return Arrays.asList(new Object[][] { //
                { "select bitmap_uuid_to_array(subtract_bitmap_uuid_distinct(uuid1, uuid2)) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select bitmap_uuid_to_array(subtract_bitmap_uuid(uuid1, uuid2)) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select subtract_bitmap_uuid_value_all(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select subtract_bitmap_value(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select bitmap_uuid_to_array(subtract_bitmap_uuid_distinct(uuid1, uuid2)) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 },
                { "select bitmap_uuid_to_array(subtract_bitmap_uuid(uuid1, uuid2)) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 },
                { "select subtract_bitmap_uuid_value_all(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 },
                { "select subtract_bitmap_value(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 } });
    }

    private void testSubtractBimapLimitOffsetValuesFunction() throws SQLException {
        Collection<Object[]> subtractBimapCountValues = testSubtractBimapLimitOffsetSql();
        for (Object[] object : subtractBimapCountValues) {
            String valueTmp = (String) object[0];
            int rowsSize = (int) object[1];

            int limit = 100;
            int offset = 0;
            int pageSize = subtract.size() / limit + 1;
            for (int page = 1; page <= pageSize; page++) {
                offset = (page - 1) * limit;
                String valueSql = valueTmp.replace("limit", limit + "").replace("offset", offset + "");
                List<Row> rows = ExecAndComp.queryModel(getProject(), valueSql).collectAsList();
                Assert.assertEquals(rowsSize, rows.size());
                for (Row row : rows) {
                    int _offset = offset;
                    List<Object> valueResult = row.getList(0);
                    Assert.assertTrue(CollectionUtils.isNotEmpty(valueResult));
                    for (Object value : valueResult) {
                        Assert.assertEquals(subtract.get(_offset).toString(), value.toString());
                        _offset += 1;
                    }
                }
            }
        }
    }

    private Collection<Object[]> testSubtractBimapLimitOffsetSql() {
        return Arrays.asList(new Object[][] { //
                { "select subtract_bitmap_uuid_value(uuid1, uuid2, limit, offset) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select subtract_bitmap_uuid_value(uuid1, uuid2, limit, offset) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 } });
    }

    private void testSubtractBimapCountFunction() throws SQLException {
        Collection<Object[]> subtractBimapCountValues = testSubtractBimapCountSql();
        for (Object[] object : subtractBimapCountValues) {
            String valueSql = (String) object[0];
            int rowsSize = (int) object[1];

            List<Row> rows = ExecAndComp.queryModel(getProject(), valueSql).collectAsList();
            Assert.assertEquals(rowsSize, rows.size());
            for (Row row : rows) {
                int countResult = row.getInt(0);
                Assert.assertEquals(subtract.size(), countResult);
            }
        }
    }

    private Collection<Object[]> testSubtractBimapCountSql() {
        return Arrays.asList(new Object[][] { //
                { "select subtract_bitmap_uuid_count(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 1 },
                { "select subtract_bitmap_uuid_count(uuid1, uuid2) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)", 2 } });
    }

    private void testSubtractBimapLimitOffsetValuesErrorFunction() {
        Collection<String> subtractBimapCountValues = testSubtractBimapLimitOffsetErrorSql();
        for (String valueTmp : subtractBimapCountValues) {
            try {
                String valueSql = valueTmp.replace("limit", "-1").replace("offset", "0");
                ExecAndComp.queryModel(getProject(), valueSql).collectAsList();
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
                Assert.assertEquals("both limit and offset must be >= 0", e.getCause().getMessage());
            }

            try {
                String valueSql = valueTmp.replace("limit", "0").replace("offset", "-1");
                ExecAndComp.queryModel(getProject(), valueSql).collectAsList();
            } catch (Exception e) {
                Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
                Assert.assertEquals("both limit and offset must be >= 0", e.getCause().getMessage());
            }
        }
    }

    private Collection<String> testSubtractBimapLimitOffsetErrorSql() {
        return Arrays.asList(//
                "select subtract_bitmap_uuid_value(uuid1, uuid2, limit, offset) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)",
                "select subtract_bitmap_uuid_value(uuid1, uuid2, limit, offset) uuid\n"
                        + "from (select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT\n" //
                        + "      union all\n"
                        + "      select intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['ABIN'])    as uuid1,\n"
                        + "             intersect_bitmap_uuid(SELLER_ID, LSTG_FORMAT_NAME, array['Auction']) as uuid2\n"
                        + "      from TEST_KYLIN_FACT)");
    }
}
