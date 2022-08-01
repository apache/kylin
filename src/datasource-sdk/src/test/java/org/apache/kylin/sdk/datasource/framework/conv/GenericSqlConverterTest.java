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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class GenericSqlConverterTest {

    @Test
    public void testConvertSql() throws SQLException {
        GenericSqlConverter sqlConverter = new GenericSqlConverter();
        // test function
        List<String> functionTestSqls = new LinkedList<>();
        functionTestSqls.add("SELECT MIN(C1)\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT EXP(AVG(LN(EXTRACT(DOY FROM CAST('2018-03-20' AS DATE)))))\nFROM TEST_SUITE");
        functionTestSqls.add(
                "SELECT CASE WHEN SUM(C1 - C1 + 1) = 1 THEN 0 ELSE (SUM(C1 * C1) - SUM(C1) * SUM(C1) / SUM(C1 - C1 + 1)) / (SUM(C1 - C1 + 1) - 1) END\n"
                        + "FROM TEST_SUITE");
        functionTestSqls.add("SELECT EXTRACT(DAY FROM CAST('2018-03-20' AS DATE))\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT FIRST_VALUE(C1) OVER (ORDER BY C1)\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT SUBSTR('world', 1, CAST(2 AS INTEGER))\nFROM TEST_SUITE");
        functionTestSqls.add("SELECT 2 - TRUNC(2 / NULLIF(3, 0)) * 3\nFROM TEST_SUITE");
        functionTestSqls.add(
                "SELECT CASE WHEN SUBSTRING('hello' FROM CAST(LENGTH('llo') - LENGTH('llo') + 1 AS INTEGER) FOR CAST(LENGTH('llo') AS INTEGER)) = 'llo' THEN 1 ELSE 0 END\n"
                        + "FROM TEST_SUITE");
        functionTestSqls
                .add("SELECT SUBSTRING('world' FROM CAST(LENGTH('world') - 3 + 1 AS INTEGER) FOR CAST(3 AS INTEGER))\n"
                        + "FROM TEST_SUITE");

        for (String originSql : functionTestSqls) {
            testSqlConvert(originSql, "testing", "default", sqlConverter);
        }
        // test datatype
        List<String> typeTestSqls = new LinkedList<>();
        typeTestSqls.add("SELECT CAST(PRICE AS DOUBLE PRECISION)\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(PRICE AS DECIMAL(19, 4))\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(PRICE AS DECIMAL(19))\n" + "FROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(BYTE AS BIT(8))\nFROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(BYTE AS VARCHAR(1024))\n" + "FROM \"default\".FACT");
        for (String originSql : typeTestSqls) {
            testSqlConvert(originSql, "testing", "default", sqlConverter);
        }
    }

    @Test
    public void testConvertParallel() {
        final GenericSqlConverter sqlConverter = new GenericSqlConverter();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        String sql = "SELECT CORR(C1,   C2)\nFROM TEST_SUITE";
        String expectedConvertedSql = "SELECT CORR(C1, C2)\nFROM TEST_SUITE";
        List<ThreadConverter> list = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            ThreadConverter t = new ThreadConverter(sqlConverter, sql);
            list.add(t);
            executor.submit(t);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(30L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail("Exception not finished convert sql int 30 seconds");
        }
        for (ThreadConverter t : list) {
            Assert.assertEquals(expectedConvertedSql, t.getConvertedSql());
        }
    }

    static class ThreadConverter implements Runnable {
        private String sql;
        private String convertedSql;
        private GenericSqlConverter sqlConverter;

        ThreadConverter(GenericSqlConverter sqlConverter, String sql) {
            this.sqlConverter = sqlConverter;
            this.sql = sql;
        }

        @Override
        public void run() {
            try {
                convertedSql = sqlConverter.convertSql(sql, "testing", "default");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public String getConvertedSql() {
            return convertedSql;
        }
    }

    private void testSqlConvert(String originSql, String sourceDialect, String targetDialect,
            GenericSqlConverter sqlConverter) throws SQLException {
        String convertedSql = sqlConverter.convertSql(originSql, sourceDialect, targetDialect);
        String revertSql = sqlConverter.convertSql(convertedSql, targetDialect, sourceDialect);
        Assert.assertEquals(originSql, revertSql);
    }

}
