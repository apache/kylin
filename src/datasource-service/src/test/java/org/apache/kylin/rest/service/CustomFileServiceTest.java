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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_JAR_TOO_LARGE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_UPLOAD_PARSER_LIMIT;
import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.jar.JarInfo;
import org.apache.kylin.metadata.jar.JarInfoManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.streaming.DataParserInfo;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

public class CustomFileServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final CustomFileService customFileService = Mockito.spy(CustomFileService.class);
    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Mock
    private AclUtil aclUtil = Mockito.spy(AclUtil.class);

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static final String JAR_TYPE = "STREAMING_CUSTOM_PARSER";
    private static String JAR_ABS_PATH;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(customFileService, "aclEvaluate", aclEvaluate);
        initJar();
    }

    public void initJar() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toString();
    }

    @Test
    public void testCheckJarLegal() throws IOException {
        {
            MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                    Files.newInputStream(Paths.get(JAR_ABS_PATH)));
            customFileService.checkJarLegal(jarFile, PROJECT, JAR_TYPE);
        }
        {
            // jar name
            MockMultipartFile jarFile = new MockMultipartFile("custom_parser.txt", "custom_parser.txt",
                    "multipart/form-data", Files.newInputStream(Paths.get(JAR_ABS_PATH)));
            Assert.assertThrows(CUSTOM_PARSER_NOT_JAR.getMsg("custom_parser.txt"), KylinException.class,
                    () -> customFileService.checkJarLegal(jarFile, PROJECT, JAR_TYPE));
        }
        {
            MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                    Files.newInputStream(Paths.get(JAR_ABS_PATH)));
            NProjectManager.getProjectConfig(PROJECT).setProperty("kylin.streaming.custom-jar-size", "1mb");
            long jarSizeMB = NProjectManager.getProjectConfig(PROJECT).getStreamingCustomJarSizeMB();
            Assert.assertEquals(1048576, jarSizeMB);
            Assert.assertThrows(CUSTOM_PARSER_JAR_TOO_LARGE.getMsg("1048576"), KylinException.class,
                    () -> customFileService.checkJarLegal(jarFile, PROJECT, JAR_TYPE));
            NProjectManager.getProjectConfig(PROJECT).setProperty("kylin.streaming.custom-jar-size", "20mb");
        }
    }

    @Test
    public void testCheckJarLegalExists() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        JarInfoManager.getInstance(getTestConfig(), PROJECT)
                .createJarInfo(new JarInfo(PROJECT, JAR_NAME, "", STREAMING_CUSTOM_PARSER));
        Assert.assertThrows(CUSTOM_PARSER_JAR_EXISTS.getMsg(JAR_NAME), KylinException.class,
                () -> customFileService.checkJarLegal(jarFile, PROJECT, JAR_TYPE));
    }

    @Test
    public void testUploadCustomJar() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        String jar = customFileService.uploadCustomJar(jarFile, PROJECT, JAR_TYPE);
        Assert.assertNotNull(jar);
    }

    @Test
    public void testLoadParserJar() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        Set<String> classList = customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE);
        Assert.assertFalse(classList.isEmpty());
        customFileService.removeJar(PROJECT, JAR_NAME, JAR_TYPE);
        //Jar Type Error
        Assert.assertThrows(KylinException.class, () -> customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE + "1"));
    }

    @Test
    public void testRemoveJarError() {
        Assert.assertThrows(CUSTOM_PARSER_NOT_JAR.getMsg(JAR_NAME + ".txt"), KylinException.class,
                () -> customFileService.removeJar(PROJECT, JAR_NAME + ".txt", JAR_TYPE));
    }

    @Test
    public void testLoadParserJarOverLimit() throws IOException {
        MockMultipartFile jarFile = new MockMultipartFile(JAR_NAME, JAR_NAME, "multipart/form-data",
                Files.newInputStream(Paths.get(JAR_ABS_PATH)));
        DataParserManager manager = DataParserManager.getInstance(getTestConfig(), PROJECT);
        manager.initDefault();
        for (int i = 0; i < 50; i++) {
            DataParserInfo parserInfo = new DataParserInfo(PROJECT, i + "", i + "");
            manager.createDataParserInfo(parserInfo);
        }
        Assert.assertTrue(manager.listDataParserInfo().size() - 1 <= 50);
        Assert.assertThrows(
                CUSTOM_PARSER_UPLOAD_PARSER_LIMIT.getMsg(50, 3, getTestConfig().getStreamingCustomParserLimit()),
                KylinException.class, () -> customFileService.uploadJar(jarFile, PROJECT, JAR_TYPE));
    }
}
