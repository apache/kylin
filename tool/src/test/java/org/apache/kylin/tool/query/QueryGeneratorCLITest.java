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

package org.apache.kylin.tool.query;

import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryGeneratorCLITest extends LocalFileMetadataTestCase {

    public final String cubeName = "test_kylin_cube_with_slr_desc";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws Exception {
        QueryGeneratorCLI queryGeneratorCLI = new QueryGeneratorCLI();
        Pair<List<String>, double[]> result = queryGeneratorCLI.execute(cubeName, 10);
        List<String> sqls = result.getFirst();
        double[] probs = result.getSecond();
        for (int i = 0; i < sqls.size(); i++) {
            System.out.println("Accumulate Probability: " + probs[i]);
            System.out.println("SQL: " + sqls.get(i));
        }
    }
}
