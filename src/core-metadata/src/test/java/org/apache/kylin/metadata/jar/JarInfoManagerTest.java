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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_ALREADY_EXISTS_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_EXISTS_JAR;
import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JarInfoManagerTest extends NLocalFileMetadataTestCase {

    private JarInfoManager manager;
    private static final String project = "streaming_test";
    private static final String jarName = "custom_parser_test.jar";
    private static final String jarPath = "/streaming_test/jar/custom_parser_test.jar";
    private static final String jarTypeName = "STREAMING_CUSTOM_PARSER_custom_parser_test.jar";
    private static final String jarType = "STREAMING_CUSTOM_PARSER";
    private static final String test = "test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        manager = JarInfoManager.getInstance(getTestConfig(), project);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetJarInfo() {

        JarInfo jarInfo = manager.getJarInfo(JarTypeEnum.valueOf(jarType), jarName);
        Assert.assertNotNull(jarInfo);
        Assert.assertEquals(project, jarInfo.getProject());
        Assert.assertEquals(jarName, jarInfo.getJarName());
        Assert.assertEquals(jarPath, jarInfo.getJarPath());
        Assert.assertEquals(jarTypeName, jarInfo.resourceName());

        JarInfo jarInfo2 = manager.getJarInfo(STREAMING_CUSTOM_PARSER, null);
        Assert.assertNull(jarInfo2);
    }

    @Test
    public void testCreateJarInfoNull() {
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createJarInfo(null));
    }

    @Test
    public void testCreateJarInfoNullResourceName() {
        JarInfo jarInfo = new JarInfo();
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createJarInfo(jarInfo));
    }

    @Test
    public void testCreateJarInfoContains() {
        JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
        Assert.assertThrows(CUSTOM_PARSER_ALREADY_EXISTS_JAR.getMsg(jarInfo.getJarName()), KylinException.class,
                () -> manager.createJarInfo(jarInfo));
    }

    @Test
    public void testCreateJarInfo() {
        JarInfo jarInfo = manager.createJarInfo(new JarInfo(project, test, jarPath, STREAMING_CUSTOM_PARSER));
        Assert.assertNotNull(jarInfo);
        manager.removeJarInfo(STREAMING_CUSTOM_PARSER, jarInfo.getJarName());
    }

    @Test
    public void testUpdateJarInfoNotContains() {
        JarInfo jarInfo = new JarInfo(project, test + "1", jarPath, STREAMING_CUSTOM_PARSER);
        Assert.assertThrows(CUSTOM_PARSER_NOT_EXISTS_JAR.getMsg(jarInfo.getJarName()), KylinException.class,
                () -> manager.updateJarInfo(jarInfo));
    }

    @Test
    public void testUpdateJarInfo() {
        JarInfo jarInfo1 = manager.createJarInfo(new JarInfo(project, jarName + "1", jarPath, STREAMING_CUSTOM_PARSER));
        jarInfo1.setJarPath(test);
        JarInfo jarInfo2 = manager.updateJarInfo(jarInfo1);
        manager.removeJarInfo(STREAMING_CUSTOM_PARSER, jarName);
        Assert.assertEquals(test, jarInfo2.getJarPath());
    }

    @Test
    public void testRemoveJarInfo() {

        Assert.assertThrows(CUSTOM_PARSER_NOT_EXISTS_JAR.getMsg("aaa.jar"), KylinException.class,
                () -> manager.removeJarInfo(STREAMING_CUSTOM_PARSER, "aaa.jar"));

        JarInfo jarInfo = manager.createJarInfo(new JarInfo(project, jarName + "1", jarPath, STREAMING_CUSTOM_PARSER));
        Assert.assertNotNull(jarInfo);
    }

    @Test
    public void testListJarInfo() {
        {
            List<JarInfo> jarInfos = manager.listJarInfo();
            Assert.assertFalse(jarInfos.isEmpty());
        }

        {
            List<JarInfo> jarInfos = manager.listJarInfoByType(STREAMING_CUSTOM_PARSER);
            Assert.assertFalse(jarInfos.isEmpty());
        }
    }

}
