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

import java.io.File;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class ITFailfastQueryTest extends KylinTestBase {

    private static final Logger logger = LoggerFactory.getLogger(ITFailfastQueryTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
    }

    @After
    public void cleanUp() {
        QueryContext.reset();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in ITFailfastQueryTest");
        Candidate.restorePriorities();
        clean();
    }

    @Test
    public void testPartitionExceedMaxScanBytes() throws Exception {
        String key = "kylin.storage.partition.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getPartitionMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "1000");//very low threshold 

        boolean meetExpectedException = false;
        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            try {
                runSQL(sqlFile, false, false);
            } catch (Exception e) {
                if (findRoot(e) instanceof ResourceLimitExceededException) {
                    //expected
                    meetExpectedException = true;
                } else {
                    throw new RuntimeException(e);
                }
            }

            if (!meetExpectedException) {
                throw new RuntimeException("Did not meet expected exception");
            }
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testPartitionNotExceedMaxScanBytes() throws Exception {
        String key = "kylin.storage.partition.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getPartitionMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "100000");//enough threshold 

        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            runSQL(sqlFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testQueryExceedMaxScanBytes() throws Exception {
        String key = "kylin.query.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getQueryMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "1000");//very low threshold 

        boolean meetExpectedException = false;
        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            try {
                runSQL(sqlFile, false, false);
            } catch (Exception e) {
                if (findRoot(e) instanceof ResourceLimitExceededException) {
                    //expected
                    meetExpectedException = true;
                } else {
                    throw new RuntimeException(e);
                }
            }

            if (!meetExpectedException) {
                throw new RuntimeException("Did not meet expected exception");
            }
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

    @Test
    public void testQueryNotExceedMaxScanBytes() throws Exception {
        String key = "kylin.query.max-scan-bytes";
        long saved = KylinConfig.getInstanceFromEnv().getQueryMaxScanBytes();
        KylinConfig.getInstanceFromEnv().setProperty(key, "100000");//enough threshold 

        try {
            String queryFileName = getQueryFolderPrefix() + "src/test/resources/query/sql/query01.sql";
            File sqlFile = new File(queryFileName);
            runSQL(sqlFile, false, false);
        } finally {
            KylinConfig.getInstanceFromEnv().setProperty(key, String.valueOf(saved));
        }
    }

}
