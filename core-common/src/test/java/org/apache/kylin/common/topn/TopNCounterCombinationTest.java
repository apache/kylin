/*
 * Copyright (C) 2011 Clearspring Technologies, Inc. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common.topn;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TopNCounterCombinationTest extends TopNCounterTest {

    @Parameterized.Parameters
    public static Collection<Integer[]> configs() {
        //       return Arrays.asList(new Object[][] { { "inner", "unset" }, { "left", "unset" }, { "inner", "off" }, { "left", "off" }, { "inner", "on" }, { "left", "on" }, });
        return Arrays.asList(new Integer[][] {
                /*
                // with 20X space
                { 100, 10, 20 }, // top 100 among 1,000 keys (top 10%)
                { 100, 20, 20 }, // top 100 among 2,000 keys (top 5%)
                { 100, 100, 20 }, // top 100 among 10,000 keys (top 1%)
                { 100, 1000, 20 }, // top 100 among 100,000 keys (top 0.1%)
                
                */
                // with 50X space
                { 100, 10, 50 }, // top 100 among 1,000 keys (top 10%)
                { 100, 20, 50 }, // top 100 among 2,000 keys (top 5%)
                { 100, 100, 50 }, // top 100 among 10,000 keys (top 1%)
                { 100, 1000, 50 }, // top 100 among 100,000 keys (top 0.1%)

                /*
                // with 100X space
                { 100, 10, 100 }, // top 100 among 1,000 keys (top 10%)
                { 100, 20, 100 }, // top 100 among 2,000 keys (top 5%)
                { 100, 100, 100 }, // top 100 among 10,000 keys (top 1%)
                { 100, 1000, 100 }, // top 100 among 100,000 keys (top 0.1%)
                */
        });
    }

    public TopNCounterCombinationTest(int topK, int keySpaceRate, int spaceSavingRate) throws Exception {
        super();
        this.TOP_K = topK;
        this.KEY_SPACE = TOP_K * keySpaceRate;
        this.SPACE_SAVING_ROOM = spaceSavingRate;
        TOTAL_RECORDS = 1000000; // 1 million
        this.PARALLEL = 50;
        this.verbose = false;
    }
}
