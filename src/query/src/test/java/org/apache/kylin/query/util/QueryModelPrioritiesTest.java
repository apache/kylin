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

public class QueryModelPrioritiesTest {

    @Test
    public void testGetPriorities() {
        Assert.assertEquals("", getModelHints("select MODEL_PRIORITY(model1)"));
        Assert.assertEquals("", getModelHints("select MODEL_PRIORITY(model1)   */"));
        Assert.assertEquals("", getModelHints("select /*+ MODEL_PRIORITY()"));
        Assert.assertEquals("", getModelHints("select /*+ MODEL_PRIORITY111(model1)"));
        Assert.assertEquals("", getModelHints("select /* MODEL_PRIORITY(model1)"));
        Assert.assertEquals("MODEL1", getModelHints("select /*+ MODEL_PRIORITY(model1) */"));
        Assert.assertEquals("MODEL1,MODEL2", getModelHints("select /*+ MODEL_PRIORITY(model1, model2)   */"));
        Assert.assertEquals("MODEL1,MODEL2,MODEL3",
                getModelHints("select /*+ MODEL_PRIORITY(model1, model2,     model3)*/"));
        Assert.assertEquals("MODEL1", getModelHints("select   /*+   MODEL_PRIORITY(model1)  */ a from tbl"));
        Assert.assertEquals("MODEL1,MODEL2", getModelHints(
                "select a from table inner join (select /*+ MODEL_PRIORITY(model1, model2) */b from table)"));
        Assert.assertEquals("MODEL3", getModelHints(
                "select /*+MODEL_PRIORITY(model3)*/ LO_COMMITDATE,LO_CUSTKEY,count(LO_LINENUMBER) from  SSB.LINEORDER  where LO_COMMITDATE in(select /*+ MODEL_PRIORITY(model1)*/LO_COMMITDATE from SSB.LINEORDER) group by LO_CUSTKEY,LO_COMMITDATE"));
    }

    private String getModelHints(String sql) {
        return String.join(",", QueryModelPriorities.getModelPrioritiesFromComment(sql));
    }

    private void assertModelHints(String expected, String sql) {
        Assert.assertEquals(expected, getModelHints(sql));
    }

    @Test
    public void testCubePriorityWithComment() {
        //illegal CubePriority, cubepriority wont be recognized
        assertModelHints("", "-- cubepriority(aaa,bbb)\n" + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        //illegal CubePriority, no blank space between '('
        assertModelHints("", "-- CubePriority (aaa,bbb)\n" + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        // case insensitive
        assertModelHints("AAA,BBB", "-- CubePriority(aaa,bbb)\n" + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n");
        //recog first matched
        String sql = "   --  CubePriority(kylin_1,kylin_2,kylin_1,kylin_3)   \n"
                + "SELECT * FROM KYLIN_SALES as KYLIN_SALES\n"
                + " -- CubePriority(kylin_4,kylin_5)   \n -- CubePriority(kylin_4,kylin_5)\n";
        assertModelHints("KYLIN_1,KYLIN_2,KYLIN_1,KYLIN_3", sql);
        // has both model_priority and CubePriority
        Assert.assertEquals("MODEL1",
                getModelHints("-- CubePriority(aaa,bbb) \nselect   /*+   MODEL_PRIORITY(model1)  */ a from tbl"));
    }

}
