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

package org.apache.kylin.metadata.jar;

import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import org.junit.Assert;
import org.junit.Test;

public class JarInfoTest {

    private final String project = "streaming_test";
    private final String jarName = "test.jar";
    private final String jarPath = "/test/test.jar";

    @Test
    public void testCreate() {
        {
            JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
            Assert.assertEquals(project, jarInfo.getProject());
            Assert.assertEquals(jarName, jarInfo.getJarName());
            Assert.assertEquals(jarPath, jarInfo.getJarPath());
            Assert.assertEquals(STREAMING_CUSTOM_PARSER, jarInfo.getJarType());
            Assert.assertEquals("/streaming_test/jar/" + STREAMING_CUSTOM_PARSER + "_" + jarName + ".json",
                    jarInfo.getResourcePath());
        }

        {
            JarInfo jarInfo = new JarInfo();
            Assert.assertNull(jarInfo.getProject());
            Assert.assertNull(jarInfo.getJarName());
            Assert.assertNull(jarInfo.getJarPath());
            Assert.assertNull(jarInfo.getJarType());
            Assert.assertNull(jarInfo.resourceName());
        }
    }

}
