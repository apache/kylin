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

import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.util.ExecAndComp;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.Test;

import scala.collection.JavaConversions;

public class CalciteDynamicTest extends NLocalWithSparkSessionTest {

    @After
    public void after() throws Exception {
        JobContextUtil.cleanUp();
    }

    @Test
    public void testCalciteGroupByDynamicParam() throws Exception {
        fullBuild("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        populateSSWithCSVData(KylinConfig.getInstanceFromEnv(), getProject(), SparderEnv.getSparkSession());
        String sqlOrigin = "SELECT (case when 1=1 then SELLER_ID else TRANS_ID end) as id,  SUM(price) as PRICE\n"
                + "FROM TEST_KYLIN_FACT\n" + "GROUP BY (case when 1=1 then SELLER_ID else TRANS_ID end) limit 5";
        String parameter = "1";
        // benchmark
        List<List<String>> benchmark = ExecAndComp.queryCubeWithJDBC(getProject(), sqlOrigin);
        // setTimestamp
        String sqlWithPlaceholder = sqlOrigin.replace("case when 1=1", "case when ?=1");
        List<Row> rows = ExecAndComp
                .queryModel(getProject(), sqlWithPlaceholder, Arrays.asList(new String[] { parameter, parameter }))
                .collectAsList();
        List<List<String>> results = transformToString(rows);
        for (int i = 0; i < benchmark.size(); i++) {
            if (!ListUtils.isEqualList(benchmark.get(i), results.get(i))) {
                String expected = String.join(",", benchmark.get(i));
                String actual1 = String.join(",", results.get(i));
                fail("expected: " + expected + ", results: " + actual1);
            }
        }
    }

    private List<List<String>> transformToString(List<Row> rows) {
        return rows.stream().map(row -> JavaConversions.seqAsJavaList(row.toSeq()).stream().map(r -> {
            if (r == null) {
                return null;
            } else {
                String s = r.toString();
                if (r instanceof Timestamp) {
                    return s.substring(0, s.length() - 2);
                } else {
                    return s;
                }
            }
        }).collect(Collectors.toList())).collect(Collectors.toList());
    }
}
