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

import java.sql.DriverManager;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.ext.ClassLoaderUtils;
import org.apache.kylin.junit.SparkTestRunner;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.routing.Candidate;
import org.apache.spark.sql.SparderEnv;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@Ignore("@RunWith(SparkTestRunner.class) is contained by ITCombination2Test")
@RunWith(SparkTestRunner.class)
public class ITKylinQuery2Test extends ITKylinQueryTest {

    private static final Logger logger = LoggerFactory.getLogger(ITKylinQuery2Test.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in ITKylinQuery2Test");
        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.HYBRID, 0);
        priorities.put(RealizationType.CUBE, 0);
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        Candidate.setPriorities(priorities);

        joinType = "left";

        setupAll();
    }

    protected static void setupAll() throws Exception {
        //setup env
        HBaseMetadataTestCase.staticCreateTestMetadata();
        config = KylinConfig.getInstanceFromEnv();

        //setup cube conn
        String project = ProjectInstance.DEFAULT_PROJECT_NAME;
        cubeConnection = QueryConnection.getConnection(project);

        //setup h2
        h2Connection = DriverManager.getConnection("jdbc:h2:mem:db" + (h2InstanceCount++) + ";CACHE_SIZE=32072", "sa",
                "");
        // Load H2 Tables (inner join)
        H2Database h2DB = new H2Database(h2Connection, config, project);
        h2DB.loadAllTables();

        // init spark
        ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader());
        SparderEnv.init();
        Thread.currentThread().setContextClassLoader(originClassLoader);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in ITKylin2QueryTest");
        Candidate.restorePriorities();
        clean();
    }

    @Override
    @Test
    public void testTimeoutQuery() throws Exception {
        logger.info("TimeoutQuery ignored.");
    }

    @Override
    @Test
    public void testExpressionQuery() throws Exception {
        logger.info("ExpressionQuery ignored.");
    }

    @Override
    @Test
    public void testStreamingTableQuery() throws Exception {
        logger.info("StreamingTableQuery ignored.");
    }
}
                                                  