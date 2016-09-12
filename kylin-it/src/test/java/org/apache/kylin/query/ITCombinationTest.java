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

package org.apache.kylin.query;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.storage.hbase.HBaseStorage;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
public class ITCombinationTest extends ITKylinQueryTest {

    @BeforeClass
    public static void setUp() throws SQLException {
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        Candidate.setPriorities(priorities);

        printInfo("setUp in ITCombinationTest");
    }

    @AfterClass
    public static void tearDown() {
        printInfo("tearDown in ITCombinationTest");
        clean();
        HBaseStorage.overwriteStorageQuery = null;
        Candidate.restorePriorities();
    }

    /**
     * return all config combinations, where first setting specifies join type
     * (inner or left), and the second setting specifies whether to force using
     * coprocessors(on, off or unset).
     */
    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { "inner", "on", "v2", false }, { "left", "on", "v1", false }, { "left", "on", "v2", false }, { "inner", "on", "v2", true }, { "left", "on", "v2", true } });
    }

    public ITCombinationTest(String joinType, String coprocessorToggle, String queryEngine, boolean excludeViewCubes) throws Exception {

        printInfo("Into combination join type: " + joinType + ", coprocessor toggle: " + coprocessorToggle + ", query engine: " + queryEngine + ", excludeViewCubes: " + excludeViewCubes);

        ITKylinQueryTest.clean();

        ITKylinQueryTest.joinType = joinType;
        ITKylinQueryTest.setupAll();

        if (coprocessorToggle.equals("on")) {
            ObserverEnabler.forceCoprocessorOn();
        } else if (coprocessorToggle.equals("off")) {
            ObserverEnabler.forceCoprocessorOff();
        } else if (coprocessorToggle.equals("unset")) {
            // unset
        }

        RemoveBlackoutRealizationsRule.blackList.clear();
        if (excludeViewCubes) {
            RemoveBlackoutRealizationsRule.blackList.add("CUBE[name=test_kylin_cube_with_view_left_join_empty]");
            RemoveBlackoutRealizationsRule.blackList.add("CUBE[name=test_kylin_cube_with_view_inner_join_empty]");
        }

        if ("v1".equalsIgnoreCase(queryEngine))
            HBaseStorage.overwriteStorageQuery = HBaseStorage.v1CubeStorageQuery;
        else
            HBaseStorage.overwriteStorageQuery = null;
    }
}
