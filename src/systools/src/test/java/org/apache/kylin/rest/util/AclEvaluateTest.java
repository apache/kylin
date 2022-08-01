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
 *
 */

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PROJECT_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ KylinConfig.class, NProjectManager.class })
public class AclEvaluateTest {

    private AclEvaluate aclEvaluate;

    @Before
    public void setUp() throws Exception {
        aclEvaluate = new AclEvaluate();
    }

    @Test
    public void testGetProjectInstance_throwsException() {
        try {
            ReflectionTestUtils.invokeMethod(aclEvaluate, "getProjectInstance", "");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(EMPTY_PROJECT_NAME.toErrorCode().getCodeString(), ((KylinException) e).getErrorCodeString());
        }

        PowerMockito.mockStatic(KylinConfig.class);
        PowerMockito.mockStatic(NProjectManager.class);
        try {
            KylinConfig kylinConfig = mock(KylinConfig.class);
            NProjectManager nProjectManager = mock(NProjectManager.class);
            PowerMockito.when(KylinConfig.getInstanceFromEnv()).thenReturn(kylinConfig);
            PowerMockito.when(NProjectManager.getInstance(any())).thenReturn(nProjectManager);
            when(nProjectManager.getProject(anyString())).thenReturn(null);
            ReflectionTestUtils.invokeMethod(aclEvaluate, "getProjectInstance", "test_project_name");
        } catch (Exception e) {
            assertTrue(e instanceof KylinException);
            assertEquals(PROJECT_NOT_EXIST.getCodeMsg("test_project_name"), e.getLocalizedMessage());
        }
    }
}
