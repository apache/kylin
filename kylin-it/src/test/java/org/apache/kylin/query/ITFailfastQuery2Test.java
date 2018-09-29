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

import java.util.Map;

import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.ext.ClassLoaderUtils;
import org.apache.kylin.junit.SparkTestRunner;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.sql.SparderEnv;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@RunWith(SparkTestRunner.class)
public class ITFailfastQuery2Test extends ITFailfastQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(ITFailfastQuery2Test.class);

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITFailfastQueryTest");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        Candidate.setPriorities(priorities);
        joinType = "left";
        setupAll();

        // init spark
        ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SparderEnv.init();
        Thread.currentThread().setContextClassLoader(originClassLoader);
    }

    @After
    public void cleanUp() {
        QueryContextFacade.resetCurrent();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in ITFailfastQuery2Test");
        Candidate.restorePriorities();
        clean();
    }

    @Override
    @Test
    public void testQueryExceedMaxScanBytes() throws Exception {
        logger.info("testQueryExceedMaxScanBytes ignored");
    }

    @Override
    @Test
    public void testQueryNotExceedMaxScanBytes() throws Exception {
        logger.info("testQueryNotExceedMaxScanBytes ignored");
    }
}
