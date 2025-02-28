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

import org.apache.kylin.util.ExecAndComp.CompareLevel;
import org.apache.kylin.util.SuggestTestBase;
import org.junit.Test;

public class AutoTdvtTest extends SuggestTestBase {

    @Override
    public String getProject() {
        return "tdvt";
    }

    @Test
    public void testTdvt() throws Exception {
        new TestScenario(CompareLevel.NONE, "sql_tdvt").execute();
    }

    @Test
    public void testSameLevelOfTdvt() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_tdvt/same_level").execute();
    }

    @Test
    public void testDateFamily() throws Exception {
        overwriteSystemProp("kylin.query.calcite.extras-props.conformance", "LENIENT");
        new TestScenario(CompareLevel.SAME, "sql_tdvt/sql_datefamily").execute();
    }

    @Test
    public void testNestedSelectStar() throws Exception {
        new TestScenario(CompareLevel.SAME, "sql_tdvt/nested_select_star").execute();
    }

}
