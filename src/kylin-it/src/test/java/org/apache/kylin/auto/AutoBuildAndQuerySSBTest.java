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

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AutoBuildAndQuerySSBTest extends SuggestTestBase {

    public AutoBuildAndQuerySSBTest(int storageType) {
        this.storageType = storageType;
    }

    @Parameterized.Parameters
    public static Integer[] storageTypes() {
        return new Integer[] { 1, 3 };
    }

    private int storageType;

    @Test
    public void testDifferentJoinOrder() throws Exception {
        final String TEST_FOLDER = "query/sql_joinorder";
        proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.NONE, TEST_FOLDER, 0, 1)));
        exchangeStorageType(getProject(), storageType);
        TestScenario testQueries = new TestScenario(CompareLevel.SAME, TEST_FOLDER, 1, 5);
        collectQueries(Lists.newArrayList(testQueries));
        buildAndCompare(testQueries);
    }

    @Test
    public void testCollectToIterator() throws Exception {
        overwriteSystemProp("kylin.query.use-iterable-collect", "true");
        final String TEST_FOLDER = "query/sql_joinorder";
        proposeWithSmartMaster(getProject(),
                Lists.newArrayList(new TestScenario(CompareLevel.NONE, TEST_FOLDER, 0, 1)));
        exchangeStorageType(getProject(), storageType);
        TestScenario testQueries = new TestScenario(CompareLevel.SAME, TEST_FOLDER, 1, 5);
        collectQueries(Lists.newArrayList(testQueries));
        Assertions.assertAll(testQueries::execute);
    }

    @Test
    public void testScalarSubqueryJoin() {
        overwriteSystemProp("kylin.query.scalar-subquery-join-enabled", "TRUE");
        final TestScenario scenario = new TestScenario(CompareLevel.SAME, "query/sql_scalar_subquery_ci");
        Assertions.assertAll(scenario::execute);
    }

    @Override
    public String getProject() {
        return "ssb";
    }

}
