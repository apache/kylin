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

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

public class PostgresqlSqlConverterTest {

    @Test
    public void testConvertSql() throws SQLException {
        GenericSqlConverter sqlConverter = new GenericSqlConverter();
        // test function
        List<String> functionTestSqls = new LinkedList<>();
        functionTestSqls.add("SELECT SUBSTR('world', 1, 2)\nFROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT LENGHT('kylin')\nFROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT LOG(200.0) AS \"Base 10 Logarithm");
        functionTestSqls.add("SELECT LOG(2.0,16) AS \"Base 2 Logarithm");
        functionTestSqls.add("SELECT CAST('2015-01-01' AS DATE) - CAST('01-OCT-2015' AS DATE)");
        functionTestSqls.add("SELECT CAST('2015-01-01' AS DATE) + (12 * INTERVAL '1 DAY') AS NEW_DATE");
        functionTestSqls.add("SELECT CAST('2015-01-01' AS DATE) + (12 * INTERVAL '1 MONTH') AS NEW_DATE");
        functionTestSqls.add("SELECT EXTRACT(ISODOW FROM DATE '2016-12-12')");
        functionTestSqls.add("SELECT RANDOM() * 10 + 1 AS \"RAND_1_10\"");
        functionTestSqls.add("SELECT RTRIM('enterprise', 'e')");
        functionTestSqls.add("SELECT LTRIM('testltrim', 'best')");
        functionTestSqls.add("SELECT EXTRACT(WEEK FROM TIMESTAMP '2001-02-16 20:38:40')");
        functionTestSqls.add("SELECT EXP(AVG(LN(2.0)))\n" +
                "FROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT EXTRACT(DOY FROM CAST('2018-03-20' AS DATE)))\nFROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT CASE WHEN SUM(\"C1\" - \"C1\" + 1) = 1 THEN 0 ELSE (SUM(\"C1\" * \"C1\") - SUM(\"C1\") * SUM(\"C1\") / SUM(\"C1\" - \"C1\" + 1)) / (SUM(\"C1\" - \"C1\" + 1) - 1) END\n" +
                "FROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT EXTRACT(DAY FROM CAST('2018-03-20' AS DATE))\nFROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT FIRST_VALUE(\"C1\") OVER (ORDER BY \"C1\")\nFROM \"TEST_SUITE\"");

        functionTestSqls.add("SELECT 2 - TRUNC(2 / NULLIF(3, 0)) * 3\nFROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT CASE WHEN SUBSTR('hello', 3, 3) = 'llo' THEN 1 ELSE 0 END\n" +
                "FROM \"TEST_SUITE\"");
        functionTestSqls.add("SELECT SUBSTR('world', 1, 3)\n" +
                "FROM \"TEST_SUITE\"");

        for (String originSql : functionTestSqls) {
            testSqlConvert(originSql, "postgresql", "default", sqlConverter);
        }
        // test datatype
        List<String> typeTestSqls = new LinkedList<>();
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DOUBLE PRECISION)\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DECIMAL(19, 4))\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DECIMAL(19))\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(BYTE AS BIT(8))\nFROM \"default\".FACT");
        typeTestSqls.add("SELECT CAST(\"BYTE\" AS VARCHAR(1024))\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(TinyINT AS BIT(8))\nFROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(Binary AS BYTEA)\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(Float AS REAL)\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(INT AS INTEGER)\n" +
                "FROM \"default\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(TimeStamp AS TIMESTAMPTZ)\n" +
                "FROM \"default\".\"FACT\"");

        for (String originSql : typeTestSqls) {
            testSqlConvert(originSql, "postgresql", "default", sqlConverter);
        }
    }

    private void testSqlConvert(String originSql, String sourceDialect, String targetDialect, GenericSqlConverter sqlConverter) throws SQLException {
        String convertedSql = sqlConverter.convertSql(originSql, sourceDialect, targetDialect);
        String revertSql = sqlConverter.convertSql(convertedSql, targetDialect, sourceDialect);
        Assert.assertEquals(originSql, revertSql);
    }
}
