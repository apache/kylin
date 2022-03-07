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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryModelPrioritiesTest {
    @Test
    public void testGetPriorities() {
        assertEquals("", getModelHints("select MODEL_PRIORITY(model1)"));
        assertEquals("", getModelHints("select MODEL_PRIORITY(model1)   */"));
        assertEquals("", getModelHints("select /*+ MODEL_PRIORITY()"));
        assertEquals("", getModelHints("select /*+ MODEL_PRIORITY111(model1)"));
        assertEquals("", getModelHints("select /* MODEL_PRIORITY(model1)"));
        assertEquals("model1", getModelHints("select /*+ MODEL_PRIORITY(model1) */"));
        assertEquals("model1,model2", getModelHints("select /*+ MODEL_PRIORITY(model1, model2)   */"));
        assertEquals("model1,model2,model3", getModelHints("select /*+ MODEL_PRIORITY(model1, model2,     model3)*/"));
        assertEquals("model1", getModelHints("select   /*+   MODEL_PRIORITY(model1)  */ a from tbl"));
        assertEquals("model1,model2", getModelHints("select a from table inner join (select /*+ MODEL_PRIORITY(model1, model2) */b from table)"));
    }

    private String getModelHints(String sql) {
        return String.join(",", QueryModelPriorities.getCubePrioritiesFromComment(sql));
    }
}
