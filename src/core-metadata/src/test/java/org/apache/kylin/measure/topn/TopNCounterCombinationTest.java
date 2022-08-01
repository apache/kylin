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

package org.apache.kylin.measure.topn;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Ignore("For collecting accuracy statistics, not for functional test")
public class TopNCounterCombinationTest extends TopNCounterTest {

    @Parameterized.Parameters
    public static Collection<Integer[]> configs() {
        return Arrays.asList(new Integer[][] {
                // with 20X space
                { 10, 20 }, // top 10%
                { 20, 20 }, // top 5%
                { 100, 20 }, // top 1%
                { 1000, 20 }, // top 0.1%

                // with 50X space
                { 10, 50 }, // top 10%
                { 20, 50 }, // top 5%
                { 100, 50 }, // top 1%
                { 1000, 50 }, // top 0.1%

                // with 100X space
                { 10, 100 }, // top 10%
                { 20, 100 }, // top 5%
                { 100, 100 }, // top 1%
                { 1000, 100 }, // top 0.1%
        });
    }

    public TopNCounterCombinationTest(int keySpaceRate, int spaceSavingRate) throws Exception {
        super();
        this.TOP_K = 100;
        this.KEY_SPACE = TOP_K * keySpaceRate;
        this.SPACE_SAVING_ROOM = spaceSavingRate;
        TOTAL_RECORDS = 1000000; // 1 million
        this.PARALLEL = 10;
        this.verbose = true;
    }
}
