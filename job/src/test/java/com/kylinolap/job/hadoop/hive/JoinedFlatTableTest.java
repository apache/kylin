/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.hive;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.job.JoinedFlatTable;
import com.kylinolap.job.engine.JobEngineConfig;

/**
 * @author George Song (ysong1)
 * 
 */
public class JoinedFlatTableTest extends LocalFileMetadataTestCase {

    CubeInstance cube = null;
    JoinedFlatTableDesc intermediateTableDesc = null;
    String fakeJobUUID = "abc-def";
    CubeSegment cubeSegment = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        cube = CubeManager.getInstance(this.getTestConfig()).getCube("test_kylin_cube_with_slr_ready");
        cubeSegment = cube.getSegments().get(0);
        intermediateTableDesc = new JoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGenCreateTableDDL() {
        String ddl = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, "/tmp", fakeJobUUID);
        System.out.println(ddl);
        assertEquals(512, ddl.length());
    }

    @Test
    public void testGenDropTableDDL() {
        String ddl = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, fakeJobUUID);
        System.out.println(ddl);
        assertEquals(108, ddl.length());
    }

    @Test
    public void testGenerateInsertSql() throws IOException {
        String sql = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, fakeJobUUID, new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        System.out.println(sql);

        assertEquals(1127, sql.length());
    }


}
