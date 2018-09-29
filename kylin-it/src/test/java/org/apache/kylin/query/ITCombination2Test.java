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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kylin.junit.SparkTestRunnerWithParametersFactory;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(SparkTestRunnerWithParametersFactory.class)
public class ITCombination2Test extends ITKylinQuery2Test {

    private static final Logger logger = LoggerFactory.getLogger(ITCombination2Test.class);

    @BeforeClass
    public static void setUp() {
        logger.info("setUp in ITCombination2Test");
    }

    @AfterClass
    public static void tearDown() {
        logger.info("tearDown in ITCombination2Test");
        clean();
        Candidate.restorePriorities();
    }

    /**
     * return all config combinations, where first setting specifies join type
     * (inner or left), and the second setting specifies whether to force using
     * coprocessors(on, off or unset).
     */
    @Parameterized.Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { //
                { "inner", "on", "v2" }, //
                { "left", "on", "v2" }, //
        });
    }

    public ITCombination2Test(String joinType, String coprocessorToggle, String queryEngine) throws Exception {
        logger.info("Into combination join type: " + joinType + ", coprocessor toggle: " + coprocessorToggle + ", query engine: " + queryEngine);
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        Candidate.setPriorities(priorities);
        ITKylinQuery2Test.joinType = joinType;
        setupAll();
    }
}
