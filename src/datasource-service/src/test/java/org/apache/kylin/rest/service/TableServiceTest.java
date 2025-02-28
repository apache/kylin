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

import java.io.IOException;
import java.security.AccessControlException;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.mockup.CsvSource;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SourceFactory.class, ISourceMetadataExplorer.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.security.*", "org.apache.hadoop.*",
        "javax.security.*", "java.security.*", "javax.crypto.*", "javax.net.ssl.*" })
public class TableServiceTest extends NLocalFileMetadataTestCase {

    @Mock
    private final TableService tableService = Mockito.spy(TableService.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @InjectMocks
    private final TableExtService tableExtService = Mockito.spy(new TableExtService());

    @Before
    public void setup() throws IOException {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", Mockito.spy(AclUtil.class));
        ReflectionTestUtils.setField(tableService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(tableExtService, "tableService", tableService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadTablesSuccessfully() {
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_ACCOUNT" };
        LoadTableResponse tableResponse = new LoadTableResponse();
        List<Pair<TableDesc, TableExtDesc>> canLoadTables = tableService.extractTableMeta(tables, "default",
                tableResponse);
        Assert.assertEquals(0, tableResponse.getFailed().size());
        Assert.assertEquals(2, canLoadTables.size());
    }

    @Test
    public void testLoadTablesError() throws Exception {
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_ACCOUNT" };
        LoadTableResponse tableResponse = new LoadTableResponse();
        CsvSource csvSource = PowerMockito.mock(CsvSource.class);
        PowerMockito.whenNew(CsvSource.class);
        ISourceMetadataExplorer mockExplorer = Mockito.mock(ISourceMetadataExplorer.class);
        PowerMockito.mockStatic(SourceFactory.class);
        PowerMockito.doReturn(csvSource).when(SourceFactory.class, "getSource", Mockito.any());
        Mockito.when((csvSource).getSourceMetadataExplorer()).thenReturn(mockExplorer);
        Mockito.when(mockExplorer.loadTableMetadata(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new AccessControlException("Mock can not fetch table meta"));
        List<Pair<TableDesc, TableExtDesc>> canLoadTables = tableService.extractTableMeta(tables, "default",
                tableResponse);
        Assert.assertEquals(2, tableResponse.getFailed().size());
        Assert.assertEquals(0, canLoadTables.size());

        Assert.assertThrows(KylinException.class, () -> {
            tableService.extractTableMeta(tables, "default");
        });
    }

    @Test
    public void testLoadTablesErrorAndThrowException() throws Exception {
        String[] tables = { "DEFAULT.TEST_KYLIN_FACT", "DEFAULT.TEST_ACCOUNT" };
        CsvSource csvSource = PowerMockito.mock(CsvSource.class);
        PowerMockito.whenNew(CsvSource.class);
        ISourceMetadataExplorer mockExplorer = Mockito.mock(ISourceMetadataExplorer.class);
        PowerMockito.mockStatic(SourceFactory.class);
        PowerMockito.doReturn(csvSource).when(SourceFactory.class, "getSource", Mockito.any());
        Mockito.when((csvSource).getSourceMetadataExplorer()).thenReturn(mockExplorer);
        Mockito.when(mockExplorer.loadTableMetadata(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenThrow(new AccessControlException("Mock can not fetch table meta"));
        Assert.assertThrows(KylinException.class, () -> {
            tableService.extractTableMeta(tables, "default");
        });
    }
}
