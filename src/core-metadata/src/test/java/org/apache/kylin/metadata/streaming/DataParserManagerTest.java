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

package org.apache.kylin.metadata.streaming;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_ALREADY_EXISTS_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_NOT_EXISTS_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_PARSER;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class DataParserManagerTest extends NLocalFileMetadataTestCase {

    private DataParserManager manager;
    private static final String project = "streaming_test";
    private static final String defaultClassName = "org.apache.kylin.parser.TimedJsonStreamParser";
    private static final String jarName = "default";
    private static final String test = "test";
    private static final String usingTable = "DEFAULT.SSB_STREAMING";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        manager = DataParserManager.getInstance(getTestConfig(), project);
        manager.initDefault();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testInitDefault() {
        {
            DataParserInfo dataParserInfo = manager.getDataParserInfo(defaultClassName);
            Assert.assertTrue(Objects.nonNull(dataParserInfo));
            manager.initDefault();
            DataParserInfo dataParserInfo2 = manager.getDataParserInfo(defaultClassName);
            Assert.assertEquals(dataParserInfo, dataParserInfo2);
        }

        {
            val manager2 = DataParserManager.getInstance(getTestConfig(), "default");
            DataParserInfo dataParserInfo = manager2.getDataParserInfo(defaultClassName);
            Assert.assertNull(dataParserInfo);
            manager2.initDefault();
            DataParserInfo dataParserInfo2 = manager2.getDataParserInfo(defaultClassName);
            Assert.assertNotNull(dataParserInfo2);
        }

    }

    @Test
    public void testGetDataParserInfo() {

        DataParserInfo dataParserInfo1 = manager.getDataParserInfo(defaultClassName);
        Assert.assertNotNull(dataParserInfo1);
        Assert.assertEquals(project, dataParserInfo1.getProject());
        Assert.assertEquals(defaultClassName, dataParserInfo1.getClassName());
        Assert.assertEquals(jarName, dataParserInfo1.getJarName());
        Assert.assertNotNull(dataParserInfo1.getStreamingTables().get(0));

        DataParserInfo dataParserInfo2 = manager.getDataParserInfo(null);
        Assert.assertNull(dataParserInfo2);
    }

    @Test
    public void testCreateDataParserInfoNull() {
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createDataParserInfo(null));
    }

    @Test
    public void testCreateDataParserInfoEmptyClass() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, "", jarName);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createDataParserInfo(dataParserInfo));
    }

    @Test
    public void testCreateDataParserInfoContains() {
        DataParserInfo info = new DataParserInfo(project, defaultClassName, jarName);
        Assert.assertThrows(CUSTOM_PARSER_ALREADY_EXISTS_PARSER.getMsg(info.getClassName()), KylinException.class,
                () -> manager.createDataParserInfo(info));
    }

    @Test
    public void testUpdateDataParserInfoException() {
        DataParserInfo info = new DataParserInfo(project, test, jarName);
        Assert.assertThrows(CUSTOM_PARSER_NOT_EXISTS_PARSER.getMsg(info.getClassName()), KylinException.class,
                () -> manager.updateDataParserInfo(info));

    }

    @Test
    public void testUpdateDataParserInfo() {
        DataParserInfo dataParserInfo = new DataParserInfo(project, test, jarName);
        manager.createDataParserInfo(dataParserInfo);
        List<String> streamingTables = dataParserInfo.getStreamingTables();
        int except = streamingTables.size();
        streamingTables.add(test);
        dataParserInfo.setStreamingTables(streamingTables);
        manager.updateDataParserInfo(dataParserInfo);

        DataParserInfo dataParserInfo1 = manager.getDataParserInfo(test);
        Assert.assertEquals(except + 1, dataParserInfo1.getStreamingTables().size());
    }

    @Test
    public void testRemoveParserException() {

        // remove not exists parser
        Assert.assertThrows(CUSTOM_PARSER_NOT_EXISTS_PARSER.getMsg(test), KylinException.class,
                () -> manager.removeParser(test));

        // remove normal
        DataParserInfo dataParserInfo3 = new DataParserInfo(project, test, test);
        manager.createDataParserInfo(dataParserInfo3);
        DataParserInfo dataParserInfo4 = manager.removeParser(test);
        Assert.assertNotNull(dataParserInfo4);

        // exception
        DataParserInfo dataParserInfo5 = new DataParserInfo(project, test, test);
        dataParserInfo5.getStreamingTables().add(test);
        manager.createDataParserInfo(dataParserInfo5);
        Assert.assertThrows(
                CUSTOM_PARSER_TABLES_USE_PARSER.getMsg(StringUtils.join(dataParserInfo5.getStreamingTables(), ",")),
                KylinException.class, () -> manager.removeParser(test));
    }

    @Test
    public void testRemoveDefaultParser() {
        // remove default
        Assert.assertThrows(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER.getMsg(), KylinException.class,
                () -> manager.removeParser(defaultClassName));
    }

    @Test
    public void testRemoveJar() {
        DataParserInfo dataParserInfo1 = new DataParserInfo(project, test, test);
        manager.createDataParserInfo(dataParserInfo1);
        manager.removeJar(test);
        DataParserInfo dataParserInfo2 = manager.getDataParserInfo(test);
        Assert.assertNull(dataParserInfo2);

        DataParserInfo dataParserInfo3 = new DataParserInfo(project, test, test);
        dataParserInfo3.setStreamingTables(Lists.newArrayList("DEFAULT.SSB_STREAMING"));
        manager.createDataParserInfo(dataParserInfo3);
        Assert.assertThrows(
                CUSTOM_PARSER_TABLES_USE_JAR.getMsg(StringUtils.join(manager.getJarParserUsedTables(test), ",")),
                KylinException.class, () -> manager.removeJar(test));
    }

    @Test
    public void testRemoveDefaultJar() {
        Assert.assertThrows(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER.getMsg(), KylinException.class,
                () -> manager.removeJar("default"));
    }

    @Test
    public void testRemoveUsingTable() {
        DataParserInfo info = manager.removeUsingTable(usingTable, defaultClassName);
        Assert.assertNotNull(info);
        Assert.assertThrows(CUSTOM_PARSER_NOT_EXISTS_PARSER.getMsg(test), KylinException.class,
                () -> manager.removeUsingTable(usingTable, test));

    }

    @Test
    public void testListDataParserInfo() {
        List<DataParserInfo> list = manager.listDataParserInfo();
        Assert.assertTrue(list.size() > 0);
    }

    @Test
    public void testJarHasParser() {
        Assert.assertTrue(manager.jarHasParser(jarName));
    }
}
