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

public class PrestoSqlConverterTest {
    @Test
    public void testConvertSql() throws SQLException {
        GenericSqlConverter sqlConverter = new GenericSqlConverter();
        // test function
        String originSQL = "SELECT DAYOFWEEK(CURRENT_DATE) FROM TEST_SUITE";
        String targetSQL = sqlConverter.convertSql(originSQL, "default", "presto");
        Assert.assertEquals(targetSQL,
                "SELECT CASE WHEN DAY_OF_WEEK(\"CURRENT_DATE\") IN (1, 2, 3, 4, 5, 6) THEN DAY_OF_WEEK(\"CURRENT_DATE\") + 1 ELSE 1 END\n"
                        + "FROM \"TEST_SUITE\"");

        originSQL = "SELECT EXTRACT(DAY FROM CURRENT_DATE) FROM TEST_SUITE";
        targetSQL = sqlConverter.convertSql(originSQL, "default", "presto");
        Assert.assertEquals(targetSQL, "SELECT EXTRACT(DAY FROM \"CURRENT_DATE\")\n" + "FROM \"TEST_SUITE\"");

        originSQL = "SELECT DAYOFYEAR(CURRENT_DATE) FROM TEST_SUITE";
        targetSQL = sqlConverter.convertSql(originSQL, "default", "presto");
        Assert.assertEquals(targetSQL, "SELECT DAY_OF_YEAR(\"CURRENT_DATE\")\n" + "FROM \"TEST_SUITE\"");

        originSQL = "SELECT TIMESTAMPADD(day, -(extract(day from CURRENT_DATE)), timestampadd(month,1,CURRENT_DATE)) FROM TEST_SUITE";
        targetSQL = sqlConverter.convertSql(originSQL, "default", "presto");
        Assert.assertEquals(targetSQL,
                "SELECT DATE_ADD('day', - EXTRACT(DAY FROM \"CURRENT_DATE\"), DATE_ADD('month', 1, \"CURRENT_DATE\"))\n"
                        + "FROM \"TEST_SUITE\"");

        originSQL = "SELECT TIMESTAMPDIFF(day, date'2018-01-01', date '2018-10-10') FROM TEST_SUITE";
        targetSQL = sqlConverter.convertSql(originSQL, "default", "presto");
        Assert.assertEquals(targetSQL,
                "SELECT DATE_DIFF('day', DATE '2018-01-01', DATE '2018-10-10')\n" + "FROM \"TEST_SUITE\"");
        // test datatype
        List<String> typeTestSqls = new LinkedList<>();
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DOUBLE)\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DECIMAL(19, 4))\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(\"PRICE\" AS DECIMAL(19))\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(BYTE AS BIT(8))\nFROM \"DEFAULT\".FACT");
        typeTestSqls.add("SELECT CAST(\"BYTE\" AS VARCHAR(1024))\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(TinyINT AS BIT(8))\nFROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(Binary AS BYTEA)\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(Float AS REAL)\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(INT AS INTEGER)\n" + "FROM \"DEFAULT\".\"FACT\"");
        typeTestSqls.add("SELECT CAST(TimeStamp AS TIMESTAMPTZ)\n" + "FROM \"DEFAULT\".\"FACT\"");

        for (String originSql : typeTestSqls) {
            testSqlConvert(originSql, "presto", "default", sqlConverter);
        }
    }

    private void testSqlConvert(String originSql, String sourceDialect, String targetDialect,
            GenericSqlConverter sqlConverter) throws SQLException {
        String convertedSql = sqlConverter.convertSql(originSql, sourceDialect, targetDialect);
        String revertSql = sqlConverter.convertSql(convertedSql, targetDialect, sourceDialect);
        Assert.assertEquals(originSql, revertSql);
    }
}
