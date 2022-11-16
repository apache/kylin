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

import java.lang.reflect.AccessibleObject;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Unsafe.class, NProjectManager.class })
@PowerMockIgnore({ "javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*",
        "javax.script.*" })
public class RoutineToolMockTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        createTestMetadata();

        PowerMockito.mockStatic(Unsafe.class);
        PowerMockito.doThrow(new RuntimeException("System is exited")).when(Unsafe.class, "systemExit",
                Mockito.anyInt());
        PowerMockito.doCallRealMethod().when(Unsafe.class, "changeAccessibleObject",
                Mockito.any(AccessibleObject.class), Mockito.anyBoolean());
        PowerMockito.doCallRealMethod().when(Unsafe.class, "setProperty", Mockito.anyString(), Mockito.anyString());
        PowerMockito.doCallRealMethod().when(Unsafe.class, "clearProperty", Mockito.anyString());

    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test(expected = RuntimeException.class)
    public void testRoutineToolMain() {
        RoutineTool.main(new String[] {});
    }

    @Test(expected = RuntimeException.class)
    public void testFastRoutineToolMain() {
        FastRoutineTool.main(new String[] {});
    }

    @Test(expected = RuntimeException.class)
    public void testRoutineToolOOM() throws Exception {
        PowerMockito.mockStatic(NProjectManager.class);
        PowerMockito.doThrow(new OutOfMemoryError("Java heap space")).when(NProjectManager.class, "getInstance",
                Mockito.any());
        RoutineTool.main(new String[] {});
    }

    @Test(expected = RuntimeException.class)
    public void testFastRoutineToolOOM() throws Exception {
        PowerMockito.mockStatic(NProjectManager.class);
        PowerMockito.doThrow(new OutOfMemoryError("Java heap space")).when(NProjectManager.class, "getInstance",
                Mockito.any());
        FastRoutineTool.main(new String[] {});
    }
}
