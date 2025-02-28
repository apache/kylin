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

import lombok.extern.slf4j.Slf4j;

/**
 * test type related queries
 */
@Slf4j
public class AutoRelDataTypeTest extends SuggestTestBase {

    @Test
    public void testDecimalDefaultScale() throws Exception {
        overwriteSystemProp("kylin.query.engine.default-decimal-scale", "4");
        new TestScenario(CompareLevel.SAME, "query/sql_type", 0, 1).execute();
    }
}
