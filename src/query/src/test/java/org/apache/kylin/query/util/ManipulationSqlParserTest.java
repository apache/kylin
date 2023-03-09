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

import org.junit.Assert;
import org.junit.Test;

public class ManipulationSqlParserTest {

    @Test
    public void showJob() throws ParseException {
        String sql = "SHOW JOB 70ba0c50-15b1-a40c-39a9-5f67f0f813c7-713ab396-1038-eecd-824f-3da66d531db6";
        DMLParserResult evaluate = ManipulationSqlParser.evaluate(sql);
        Assert.assertEquals(DMLParserResult.OPERATOR.SHOW, evaluate.operator);
        Assert.assertEquals(DMLParserResult.UNIT.JOB, evaluate.unit);
        Assert.assertEquals(1, evaluate.arg.size());
        Assert.assertEquals("70ba0c50-15b1-a40c-39a9-5f67f0f813c7-713ab396-1038-eecd-824f-3da66d531db6",
                evaluate.arg.get(0));
    }

    @Test
    public void triggerJob() throws ParseException {
        // 1. trigger partial build job with partition col
        String sql = "LOAD PARTITION project_name.model_test (2023-01-01, 2023-02-10)";
        DMLParserResult evaluate = ManipulationSqlParser.evaluate(sql);

        Assert.assertEquals(DMLParserResult.OPERATOR.LOAD, evaluate.operator);
        Assert.assertEquals(DMLParserResult.UNIT.PARTITION, evaluate.unit);
        Assert.assertEquals(4, evaluate.arg.size());
        Assert.assertEquals("project_name", evaluate.arg.get(0));
        Assert.assertEquals("model_test", evaluate.arg.get(1));
        Assert.assertEquals("2023-01-01", evaluate.arg.get(2));
        Assert.assertEquals("2023-02-10", evaluate.arg.get(3));

        // 2. trigger full build job
        String sql2 = "LOAD MODEL project_name.model_test2";
        DMLParserResult evaluate2 = ManipulationSqlParser.evaluate(sql2);

        Assert.assertEquals(evaluate2.operator, DMLParserResult.OPERATOR.LOAD);
        Assert.assertEquals(evaluate2.unit, DMLParserResult.UNIT.MODEL);
        Assert.assertEquals(2, evaluate2.arg.size());
        Assert.assertEquals("project_name", evaluate2.arg.get(0));
        Assert.assertEquals("model_test2", evaluate2.arg.get(1));

    }

    @Test
    public void cancelJob() throws ParseException {
        String sql = "CANCEL JOB 70ba0c50-15b1-a40c-39a9-5f67f0f813c7-713ab396-1038-eecd-824f-3da66d531db6";
        DMLParserResult evaluate = ManipulationSqlParser.evaluate(sql);

        Assert.assertEquals(DMLParserResult.OPERATOR.CANCEL, evaluate.operator);
        Assert.assertEquals(DMLParserResult.UNIT.JOB, evaluate.unit);
        Assert.assertEquals(1, evaluate.arg.size());
        Assert.assertEquals("70ba0c50-15b1-a40c-39a9-5f67f0f813c7-713ab396-1038-eecd-824f-3da66d531db6",
                evaluate.arg.get(0));
    }

    @Test
    public void deleteModel() throws ParseException {
        String sql = "DELETE MODEL project_name.model_test";
        DMLParserResult evaluate = ManipulationSqlParser.evaluate(sql);

        Assert.assertEquals(DMLParserResult.OPERATOR.DELETE, evaluate.operator);
        Assert.assertEquals(DMLParserResult.UNIT.MODEL, evaluate.unit);
        Assert.assertEquals(2, evaluate.arg.size());
        Assert.assertEquals("project_name", evaluate.arg.get(0));
        Assert.assertEquals("model_test", evaluate.arg.get(1));
    }

    @Test
    public void testError() {
        String sql = "DELETE MODELs model_test";
        Assert.assertThrows(ParseException.class, () -> ManipulationSqlParser.evaluate(sql));
    }

    @Test
    public void validDMLParserResult() {
        DMLParserResult test = new DMLParserResult();
        test.unit = DMLParserResult.UNIT.JOB;
        Assert.assertFalse(test.isValid());

        test.operator = DMLParserResult.OPERATOR.SHOW;
        Assert.assertTrue(test.isValid());
    }
}
