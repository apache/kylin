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

package org.apache.kylin.query.test;

import java.io.File;
import java.util.Map;

import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.RoutingRules.RealizationPriorityRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class IIQueryTest extends KylinQueryTest {
    @BeforeClass
    public static void setUp() throws Exception {

        KylinQueryTest.setUp();//invoke super class

        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.INVERTED_INDEX, 1);
        priorities.put(RealizationType.CUBE, 2);
        priorities.put(RealizationType.HYBRID, 2);
        RealizationPriorityRule.setPriorities(priorities);

    }

    @AfterClass
    public static void tearDown() throws Exception {
        KylinQueryTest.tearDown();//invoke super class

        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.INVERTED_INDEX, 2);
        priorities.put(RealizationType.CUBE, 1);
        priorities.put(RealizationType.HYBRID, 1);
        RealizationPriorityRule.setPriorities(priorities);
    }

    @Test
    public void testDetailedQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_ii", null, true);
    }


}
