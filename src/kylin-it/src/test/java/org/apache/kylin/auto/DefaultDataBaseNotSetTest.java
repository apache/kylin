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

package org.apache.kylin.auto;

import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class DefaultDataBaseNotSetTest extends SuggestTestBase {

    @Before
    public void setupDefaultDatabase() {
        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        npr.updateProject("ssb", copyForWrite -> copyForWrite.setDefaultDatabase("DEFAULT"));
    }

    @After
    public void recoverDefaultDatabase() {
        NProjectManager npr = NProjectManager.getInstance(kylinConfig);
        npr.updateProject("ssb", copyForWrite -> copyForWrite.setDefaultDatabase("SSB"));
    }

    @Test
    public void testUdafAndUdf() {
        String[] sqls = new String[] {
                //test Udaf
                "SELECT LO_SUPPKEY, percentile_approx(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER GROUP BY LO_SUPPKEY",
                "SELECT LO_SUPPKEY, percentile(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER GROUP BY LO_SUPPKEY",
                "SELECT LO_SUPPKEY, percentile_approx(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER "
                        + "GROUP BY LO_SUPPKEY,LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_ORDERDATE,LO_ORDERPRIOTITY,LO_SHIPPRIOTITY,"
                        + "LO_QUANTITY,LO_EXTENDEDPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE,V_REVENUE",
                "SELECT LO_SUPPKEY, percentile(LO_ORDTOTALPRICE, 0.5) AS ORDER_TOTAL_PRICE FROM SSB.P_LINEORDER "
                        + "GROUP BY LO_SUPPKEY,LO_ORDERKEY,LO_LINENUMBER,LO_CUSTKEY,LO_PARTKEY,LO_ORDERDATE,LO_ORDERPRIOTITY,LO_SHIPPRIOTITY,"
                        + "LO_QUANTITY,LO_EXTENDEDPRICE,LO_DISCOUNT,LO_REVENUE,LO_SUPPLYCOST,LO_TAX,LO_COMMITDATE,LO_SHIPMODE,V_REVENUE",
                "SELECT\n" + "  \"LO_LINENUMBER\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101]) AS \"FIRST_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960102]) AS \"SECOND_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960103]) AS \"THIRD_DAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101, 19960102]) AS \"RETENTION_ONEDAY\",\n"
                        + "    INTERSECT_COUNT(\"LO_SUPPKEY\", \"LO_ORDERDATE\", ARRAY[19960101, 19960102, 19960103]) AS \"RETENTION_TWODAY\"\n"
                        + "FROM ssb.\"P_LINEORDER\"\n" + "GROUP BY\n" + "  \"LO_LINENUMBER\"",
                //test Udf
                "select initcapb(LO_ORDERPRIOTITY),substr(LO_ORDERPRIOTITY,2),instr(LO_ORDERPRIOTITY,'A'),"
                        + "ifnull(LO_LINENUMBER,0),rlike(LO_ORDERPRIOTITY,'*') from SSB.P_LINEORDER" };
        val context = AccelerationUtil.runWithSmartContext(kylinConfig, "ssb", sqls, true);

        val modelContext = context.getModelContexts().get(0);
        Assert.assertEquals("COUNT",
                modelContext.getTargetModel().getAllMeasures().get(0).getFunction().getExpression());
        Assert.assertEquals("PERCENTILE_APPROX",
                modelContext.getTargetModel().getAllMeasures().get(1).getFunction().getExpression());
    }
}
