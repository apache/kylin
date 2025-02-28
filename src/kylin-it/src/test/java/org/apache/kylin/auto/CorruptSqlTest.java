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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.calcite.sql.type.SetopOperandTypeChecker;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.query.SQLResult;
import org.apache.kylin.util.ExecAndComp;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CorruptSqlTest extends SuggestTestBase {

    /**
     * DDL: not supported sql.
     */
    @Test
    public void testDDL() throws IOException {
        AbstractContext context = proposeWithSmartMaster("newten", Lists.newArrayList(new TestScenario("ddl-sql")));
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            final String blockMessage = value.getFailedCause().getMessage();
            Assert.assertTrue(blockMessage.contains(SQLResult.NON_SELECT_CLAUSE));
        });
    }

    /**
     * Simple SQL: this case will not propose layouts, and blocking cause is also null.
     */
    @Test
    public void testSimpleQuery() throws IOException {
        AbstractContext context = proposeWithSmartMaster("newten", Lists.newArrayList(new TestScenario("simple-sql")));
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            Assert.assertNull(value.getFailedCause());
            Assert.assertEquals(0, value.getRelatedLayouts().size());
        });
    }

    /**
     * Invalid SQL: this case will lead to parsing error.
     * Calcite 1.30 enhances the inference function of SqlNode field type in
     * which is mainly used for operations such as UNION, INTERSECT and EXCEPT
     * @see SetopOperandTypeChecker#checkOperandTypes
     */
    @Test
    public void testInvalidQuery() throws IOException {
        // see https://olapio.atlassian.net/browse/KE-42034
        // Ignored - union_error.sql
        TestScenario scenario = new TestScenario(ExecAndComp.CompareLevel.SAME, JoinType.DEFAULT, false, "parse-error",
                0, 0, Collections.singleton("union_error.sql"));
        AbstractContext context = proposeWithSmartMaster("newten", Lists.newArrayList(scenario));
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> Assert.assertTrue(value.isFailed() || value.isPending()));
    }

    /**
     * Other cases: in this case sql parsing is ok, but will lead to NPE, such as #6548, #7504
     */
    @Test
    public void testOtherCases() throws IOException {
        AbstractContext context = proposeWithSmartMaster("newten", Lists.newArrayList(new TestScenario("other_cases")));
        Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            Assert.assertTrue(value.isPending() || value.isFailed());
        });
    }

    @Test
    public void testIssueRelatedSqlEndsNormally() {

        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "LENIENT");
        try {
            AbstractContext context = proposeWithSmartMaster("newten",
                    Lists.newArrayList(new TestScenario("issues-related-sql")));
            Map<String, AccelerateInfo> accelerateInfoMap = context.getAccelerateInfoMap();
            accelerateInfoMap.forEach((key, value) -> Assert.assertFalse(value.isFailed()));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    private static final String IT_SQL_DIR = "../kylin-it/src/test/resources/corrupt-query";

    @Override
    protected String getFolder(String subFolder) {
        return IT_SQL_DIR + File.separator + subFolder;
    }
}
