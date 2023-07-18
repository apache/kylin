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

package org.apache.kylin.query.util;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PrepareSQLUtilsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testFillInParams() {
        testCase(String.class, "value1", "\'value1\'");
        testCase(Integer.class, "1", "1");
        testCase(Short.class, "1", "1");
        testCase(Long.class, "1", "1");
        testCase(Double.class, "1", "1.0");
        testCase(Float.class, "1", "1.0");
        testCase(Boolean.class, "true", "true");
        testCase(Byte.class, "1", "1");
        testCase(java.util.Date.class, "2023-04-19", "date\'2023-04-19\'");
        testCase(java.sql.Date.class, "2023-04-19", "date\'2023-04-19\'");
        testCase(java.sql.Time.class, "01:02:03", "01:02:03"); // it's a bug, fix it later
        testCase(java.sql.Timestamp.class, "2023-04-19 01:02:03", "timestamp\'2023-04-19 01:02:03.000\'");
    }

    @Test
    public void testFillInEmptyParams() {
        testCase(String.class, "", "\'\'");
        testCase(Integer.class, "", "0");
        testCase(Short.class, "", "0");
        testCase(Long.class, "", "0");
        testCase(Double.class, "", "0.0");
        testCase(Float.class, "", "0.0");
        testCase(Boolean.class, "", "false");
        testCase(Byte.class, "", "0");
        testCase(java.util.Date.class, "", "NULL");
        testCase(java.sql.Date.class, "", "NULL");
        testCase(java.sql.Time.class, "", "NULL");
        testCase(java.sql.Timestamp.class, "", "NULL");
    }

    @Test
    public void testFillInNullParams() {
        testCase(String.class, null, "NULL");
        testCase(Integer.class, null, "0");
        testCase(Short.class, null, "0");
        testCase(Long.class, null, "0");
        testCase(Double.class, null, "0.0");
        testCase(Float.class, null, "0.0");
        testCase(Boolean.class, null, "false");
        testCase(Byte.class, null, "0");
        testCase(java.util.Date.class, null, "NULL");
        testCase(java.sql.Date.class, null, "NULL");
        testCase(java.sql.Time.class, null, "NULL");
        testCase(java.sql.Timestamp.class, null, "NULL");
    }

    private static final String SQL_PATTERN = "select * from mock_table where filter_value = ";
    private static final String PLACE_HOLDER = "?";

    private void testCase(Class typeClass, String value, String expected) {
        String originalSql = SQL_PATTERN + PLACE_HOLDER;
        String expectedSql = SQL_PATTERN + expected;
        PrepareSqlStateParam param = new PrepareSqlStateParam(typeClass.getCanonicalName(), value);
        String preparedSql = PrepareSQLUtils.fillInParams(originalSql, new PrepareSqlStateParam[] {param});
        Assert.assertEquals(expectedSql, preparedSql);
    }
}
