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

package org.apache.kylin.query.aggregate;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.measure.dim.DimCountDistinctAggFunc;
import org.apache.kylin.measure.dim.DimCountDistinctCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DimCountDistinctAggFuncTest extends AbstractTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasic() {
        DimCountDistinctCounter counter = DimCountDistinctAggFunc.init();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 2; j++) {
                counter = DimCountDistinctAggFunc.add(counter, i);
                counter = DimCountDistinctAggFunc.add(counter, (double) i);
                counter = DimCountDistinctAggFunc.add(counter, (char) i);
                counter = DimCountDistinctAggFunc.add(counter, Integer.toString(i));
            }
        }

        assertEquals(40, DimCountDistinctAggFunc.result(counter));
    }

    @Test
    public void testEmpty() {
        DimCountDistinctCounter counter = DimCountDistinctAggFunc.init();
        assertEquals(0, DimCountDistinctAggFunc.result(counter));
    }

    @Test
    public void testThreshold() {
        overwriteSystemProp("kylin.query.max-dimension-count-distinct", "100");

        DimCountDistinctCounter counter = DimCountDistinctAggFunc.init();

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Cardinality of dimension exceeds the threshold: 100");

        for (int i = 0; i < 200; i++) {
            counter = DimCountDistinctAggFunc.add(counter, i);
        }
    }
}
