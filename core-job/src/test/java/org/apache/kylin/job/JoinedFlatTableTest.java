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

package org.apache.kylin.job;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
@Ignore("This test case doesn't have much value, ignore it.")
public class JoinedFlatTableTest extends LocalFileMetadataTestCase {

    CubeInstance cube = null;
    IJoinedFlatTableDesc flatTableDesc = null;
    String fakeJobUUID = "abc-def";
    CubeSegment cubeSegment = null;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_with_slr_ready");
        cubeSegment = cube.getSegments().get(0);
        flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cubeSegment);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGenCreateTableDDL() {
        String ddl = JoinedFlatTable.generateCreateTableStatement(flatTableDesc, "/tmp");
        System.out.println(ddl);

        System.out.println("The length for the ddl is " + ddl.length());
    }

    @Test
    public void testGenDropTableDDL() {
        String ddl = JoinedFlatTable.generateDropTableStatement(flatTableDesc);
        System.out.println(ddl);
        assertEquals(101, ddl.length());
    }

    @Test
    public void testGenerateInsertSql() throws IOException {
        String sqls = JoinedFlatTable.generateInsertDataStatement(flatTableDesc);
        System.out.println(sqls);

        int length = sqls.length();
        assertEquals(1437, length);
    }

}
