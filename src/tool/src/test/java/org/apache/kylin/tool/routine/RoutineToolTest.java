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
package org.apache.kylin.tool.routine;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.rest.constant.Constant;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.val;

public class RoutineToolTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Test
    public void testExecuteRoutineWithRetryTimesAndRequestFSRate() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] { "-t=10", "-r=100" });
        Assert.assertEquals(10, routineTool.getRetryTimes());
        Assert.assertEquals(100, routineTool.getRequestFSRate(), 0.1);
    }

    @Test
    public void testExecuteRoutine1() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] { "--cleanup" });
        Assert.assertTrue(routineTool.isStorageCleanup());
    }

    @Test
    public void testFastExecuteRoutine1() {
        FastRoutineTool routineTool = new FastRoutineTool();
        routineTool.execute(new String[] { "--cleanup" });
        Assert.assertTrue(routineTool.isStorageCleanup());
    }

    @Test
    public void testExecuteRoutine2() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] {});
        Assert.assertFalse(routineTool.isStorageCleanup());
    }

    @Test
    public void testFastExecuteRoutine2() {
        FastRoutineTool routineTool = new FastRoutineTool();
        routineTool.execute(new String[] {});
        Assert.assertFalse(routineTool.isStorageCleanup());
    }

    @Test
    public void testExecuteRoutineWithOptionProjects1() {
        RoutineTool routineTool = new RoutineTool();
        routineTool.execute(new String[] {});

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] {}, routineTool.getProjects());

    }

    @Test
    public void testFastExecuteRoutineWithOptionProjects1() {
        FastRoutineTool routineTool = new FastRoutineTool();
        routineTool.execute(new String[] {});

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] {}, routineTool.getProjects());

    }

    @Test
    public void testExecuteRoutineWithOptionProjects2() {
        RoutineTool routineTool = new RoutineTool();

        routineTool.execute(new String[] { "--projects=ssb" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb" }, routineTool.getProjects());
    }

    @Test
    public void testFastExecuteRoutineWithOptionProjects2() {
        FastRoutineTool routineTool = new FastRoutineTool();

        routineTool.execute(new String[] { "--projects=ssb" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb" }, routineTool.getProjects());
    }

    @Test
    public void testExecuteRoutineWithOptionProjects3() {
        RoutineTool routineTool = new RoutineTool();

        routineTool.execute(new String[] { "--projects=ssb,default" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb", "default" }, routineTool.getProjects());
    }

    @Test
    public void testFastExecuteRoutineWithOptionProjects3() {
        FastRoutineTool routineTool = new FastRoutineTool();

        routineTool.execute(new String[] { "--projects=ssb,default" });

        Assert.assertFalse(routineTool.isStorageCleanup());
        Assert.assertArrayEquals(new String[] { "ssb", "default" }, routineTool.getProjects());
    }

    @Test
    public void testCleanStreamingStats() throws Exception {
        val threadName = "executor";
        val executor = new Thread(RoutineTool::cleanStreamingStats);
        executor.setName(threadName);
        executor.start();
        executor.join();
        Assert.assertEquals(threadName, executor.getName());
    }
}
