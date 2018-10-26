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
package org.apache.kylin.source.jdbc.extensible;

import java.io.IOException;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.sdk.datasource.framework.JdbcConnector;
import org.apache.kylin.sdk.datasource.framework.SourceConnectorFactory;
import org.apache.kylin.source.H2Database;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.SourceManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcHiveMRInputTest extends TestBase {
    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata("src/test/resources/ut_meta/jdbc_source");

        JdbcConnector connector = SourceConnectorFactory.getJdbcConnector(getTestConfig());
        h2Conn = connector.getConnection();

        h2Db = new H2Database(h2Conn, getTestConfig(), "default");
        h2Db.loadAllTables();
    }

    @Test
    public void testGenSqoopCmd_Partition() throws IOException {
        ISource source = SourceManager.getSource(new JdbcSourceAware());
        IMRInput input = source.adaptToBuildEngine(IMRInput.class);
        Assert.assertNotNull(input);

        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ci_inner_join_cube");
        CubeSegment seg = cubeManager.appendSegment(cubeManager.getCube(cubeDesc.getName()),
                new SegmentRange.TSRange(System.currentTimeMillis() - 100L, System.currentTimeMillis() + 100L));
        CubeJoinedFlatTableDesc flatDesc = new CubeJoinedFlatTableDesc(seg);
        JdbcHiveMRInput.BatchCubingInputSide inputSide = (JdbcHiveMRInput.BatchCubingInputSide) input
                .getBatchCubingInputSide(flatDesc);

        AbstractExecutable executable = new MockInputSide(flatDesc, inputSide).createSqoopToFlatHiveStep("/tmp",
                cubeDesc.getName());
        Assert.assertNotNull(executable);

        String cmd = executable.getParam("cmd");
        Assert.assertTrue(cmd.contains("org.h2.Driver"));
        Assert.assertTrue(cmd.contains(
                "--boundary-query \"SELECT MIN(TEST_KYLIN_FACT.LEAF_CATEG_ID), MAX(TEST_KYLIN_FACT.LEAF_CATEG_ID)\n"
                        + "FROM \\\"DEFAULT\\\".TEST_KYLIN_FACT AS TEST_KYLIN_FACT\n"
                        + "WHERE TEST_KYLIN_FACT.CAL_DT >="));

        source.close();
    }

    @Test
    public void testGenSqoopCmd_NoPartition() throws IOException {
        ISource source = SourceManager.getSource(new JdbcSourceAware());
        IMRInput input = source.adaptToBuildEngine(IMRInput.class);
        Assert.assertNotNull(input);

        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ci_left_join_cube");
        CubeSegment seg = cubeManager.appendSegment(cubeManager.getCube(cubeDesc.getName()),
                new SegmentRange.TSRange(0L, Long.MAX_VALUE));
        CubeJoinedFlatTableDesc flatDesc = new CubeJoinedFlatTableDesc(seg);
        JdbcHiveMRInput.BatchCubingInputSide inputSide = (JdbcHiveMRInput.BatchCubingInputSide) input
                .getBatchCubingInputSide(flatDesc);

        AbstractExecutable executable = new MockInputSide(flatDesc, inputSide).createSqoopToFlatHiveStep("/tmp",
                cubeDesc.getName());
        Assert.assertNotNull(executable);
        String cmd = executable.getParam("cmd");
        Assert.assertTrue(cmd.contains("org.h2.Driver"));
        Assert.assertTrue(
                cmd.contains("--boundary-query \"SELECT MIN(TEST_KYLIN_FACT.CAL_DT), MAX(TEST_KYLIN_FACT.CAL_DT)\n"
                        + "FROM \\\"DEFAULT\\\".TEST_KYLIN_FACT AS TEST_KYLIN_FACT\""));
        source.close();
    }

    @Test
    public void testGenSqoopCmd_WithLookupShardBy() throws IOException {
        ISource source = SourceManager.getSource(new JdbcSourceAware());
        IMRInput input = source.adaptToBuildEngine(IMRInput.class);
        Assert.assertNotNull(input);

        CubeManager cubeManager = CubeManager.getInstance(getTestConfig());
        CubeDesc cubeDesc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("ut_jdbc_shard");
        CubeSegment seg = cubeManager.appendSegment(cubeManager.getCube(cubeDesc.getName()),
                new SegmentRange.TSRange(System.currentTimeMillis() - 100L, System.currentTimeMillis() + 100L));
        CubeJoinedFlatTableDesc flatDesc = new CubeJoinedFlatTableDesc(seg);
        JdbcHiveMRInput.BatchCubingInputSide inputSide = (JdbcHiveMRInput.BatchCubingInputSide) input
                .getBatchCubingInputSide(flatDesc);

        AbstractExecutable executable = new MockInputSide(flatDesc, inputSide).createSqoopToFlatHiveStep("/tmp",
                cubeDesc.getName());
        Assert.assertNotNull(executable);

        String cmd = executable.getParam("cmd");
        Assert.assertTrue(cmd.contains("org.h2.Driver"));
        Assert.assertTrue(cmd.contains(
                "--boundary-query \"SELECT MIN(TEST_CATEGORY_GROUPINGS.META_CATEG_NAME), MAX(TEST_CATEGORY_GROUPINGS.META_CATEG_NAME)\n"
                        + "FROM \\\"DEFAULT\\\".TEST_CATEGORY_GROUPINGS AS TEST_CATEGORY_GROUPINGS\""));

        source.close();
    }

    private static class MockInputSide extends JdbcHiveMRInput.BatchCubingInputSide {
        JdbcHiveMRInput.BatchCubingInputSide input;

        public MockInputSide(IJoinedFlatTableDesc flatDesc, JdbcHiveMRInput.BatchCubingInputSide input) {
            super(flatDesc, input.getDataSource());
            this.input = input;
        }

        @Override
        protected AbstractExecutable createSqoopToFlatHiveStep(String jobWorkingDir, String cubeName) {
            return input.createSqoopToFlatHiveStep(jobWorkingDir, cubeName);
        }
    }
}
