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

package org.apache.kylin.engine.mr.steps;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class FactDistinctColumnsReducerMappingTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        System.setProperty("kylin.engine.mr.uhc-reducer-count", "2");
        System.setProperty("kylin.engine.mr.per-reducer-hll-cuboid-number", "1");
        System.setProperty("kylin.engine.mr.hll-max-reducer-number", "2");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        System.clearProperty("kylin.engine.mr.uhc-reducer-count");
        System.clearProperty("kylin.engine.mr.per-reducer-hll-cuboid-number");
        System.clearProperty("kylin.engine.mr.hll-max-reducer-number");
    }

    @Test
    public void testBasics() {
        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("ci_left_join_cube");
        TblColRef aUHC = cube.getModel().findColumn("TEST_COUNT_DISTINCT_BITMAP");

        FactDistinctColumnsReducerMapping mapping = new FactDistinctColumnsReducerMapping(cube);

        int totalReducerNum = mapping.getTotalReducerNum();
        Assert.assertEquals(2, mapping.getCuboidRowCounterReducerNum());

        // check cuboid row count reducers
        Assert.assertEquals(FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER,
                mapping.getRolePlayOfReducer(totalReducerNum - 1));
        Assert.assertEquals(FactDistinctColumnsReducerMapping.MARK_FOR_HLL_COUNTER,
                mapping.getRolePlayOfReducer(totalReducerNum - 2));

        // check all dict column reducers
        int dictEnd = totalReducerNum - 2;
        for (int i = 0; i < dictEnd; i++)
            Assert.assertTrue(mapping.getRolePlayOfReducer(i) >= 0);

        // check a UHC dict column
        Assert.assertEquals(2, mapping.getReducerNumForDimCol(aUHC));
        int uhcReducerBegin = -1;
        for (int i = 0; i < dictEnd; i++) {
            if (mapping.getColForReducer(i).equals(aUHC)) {
                uhcReducerBegin = i;
                break;
            }
        }

        int[] allRolePlay = mapping.getAllRolePlaysForReducers();
        Assert.assertEquals(allRolePlay[uhcReducerBegin], allRolePlay[uhcReducerBegin + 1]);
        for (int i = 0; i < 5; i++) {
            int reducerId = mapping.getReducerIdForCol(uhcReducerBegin, i);
            Assert.assertTrue(uhcReducerBegin <= reducerId && reducerId <= uhcReducerBegin + 1);
        }
    }

    /**
     * Please see https://issues.apache.org/jira/browse/KYLIN-4083
     *
     * "539019926".hashCode() == Integer.MIN_VALUE == -2147483648
     * The absolute value of Integer.MIN_VALUE is itself
     * Then the value: (begin + Math.abs(hash) % span) may be negative
     */
    @Test
    public void testGetReducerIdForCol() {
        // set uht reducer count = 35
        System.setProperty("kylin.engine.mr.uhc-reducer-count", "35");

        CubeManager mgr = CubeManager.getInstance(getTestConfig());
        CubeInstance cube = mgr.getCube("test_kylin_cube_with_slr_1_new_segment");
        TblColRef aUHC = cube.getModel().findColumn("SELLER_ID");

        FactDistinctColumnsReducerMapping mapping = new FactDistinctColumnsReducerMapping(cube);
        // check a UHC dict column
        Assert.assertEquals(35, mapping.getReducerNumForDimCol(aUHC));

        // check all dict column reducers
        int dimCount = mapping.getDimReducerNum();
        Assert.assertEquals(43, dimCount);
        for (int i = 0; i < dimCount; i++)
            Assert.assertTrue(mapping.getRolePlayOfReducer(i) >= 0);

        int uhcReducerBegin = -1;
        for (int i = 0; i < dimCount; i++) {
            if (mapping.getColForReducer(i).equals(aUHC)) {
                uhcReducerBegin = i;
                break;
            }
        }
        Assert.assertEquals(8, uhcReducerBegin);

        // begin = 8, span = 35
        String magicString = "539019926"; // "539019926".hashCode() == Integer.MIN_VALUE == -2147483648
        // use int function: Math.abs(Integer.MIN_VALUE), the expected result:  8 + (-2147483648) %35 = -15
        // this expected result should be avoid
        Assert.assertNotEquals(-15, mapping.getReducerIdForCol(uhcReducerBegin, magicString));
        // use long function: Math.abs((long)Integer.MIN_VALUE), the expected result: 8 + 2147483648 % 35 = 31
        Assert.assertEquals(31, mapping.getReducerIdForCol(uhcReducerBegin, magicString));
    }

}
