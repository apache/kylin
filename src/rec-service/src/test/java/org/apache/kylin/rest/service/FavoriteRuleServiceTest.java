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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.ImportSqlResponse;
import org.apache.kylin.rest.response.SQLParserResponse;
import org.apache.kylin.rest.response.SQLValidateResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import lombok.val;
import lombok.var;

public class FavoriteRuleServiceTest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";

    @InjectMocks
    private final FavoriteRuleService favoriteRuleService = Mockito.spy(new FavoriteRuleService());

    @Mock
    private final AclUtil aclUtil = Mockito.spy(AclUtil.class);

    @Mock
    private final AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);

    @Mock
    protected IUserGroupService userGroupService = Mockito.spy(NUserGroupService.class);

    @Before
    public void setUp() {
        createTestMetadata();
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        ReflectionTestUtils.setField(aclEvaluate, "aclUtil", aclUtil);
        ReflectionTestUtils.setField(favoriteRuleService, "aclEvaluate", aclEvaluate);
        ReflectionTestUtils.setField(favoriteRuleService, "userGroupService", userGroupService);
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadSqls() throws IOException {
        // import multiple files
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql").toPath()));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt").toPath()));
        // add jdbc type sql
        MockMultipartFile file3 = new MockMultipartFile("sqls3.txt", "sqls3.txt", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls3.txt").toPath()));
        MockMultipartFile exceptionFile = new MockMultipartFile("exception_file.sql", "exception_file.sql",
                "text/plain", "".getBytes(StandardCharsets.UTF_8));
        MockMultipartFile errorFile = new MockMultipartFile("error_file.sql", "error_file.sql", "text/plain",
                "".getBytes(StandardCharsets.UTF_8));

        Mockito.when(favoriteRuleService.transformFileToSqls(exceptionFile, PROJECT)).thenThrow(IOException.class);
        Mockito.when(favoriteRuleService.transformFileToSqls(errorFile, PROJECT)).thenThrow(Error.class);

        SQLParserResponse result = favoriteRuleService
                .importSqls(new MultipartFile[] { file1, file2, file3, exceptionFile, errorFile }, PROJECT);
        List<ImportSqlResponse> responses = result.getData();
        Assert.assertEquals(10, responses.size());
        Assert.assertFalse(responses.get(0).isCapable());
        Assert.assertTrue(responses.get(8).isCapable());
        Assert.assertEquals(10, result.getSize());
        Assert.assertEquals(3, result.getCapableSqlNum());
        List<String> failedFilesMsg = result.getWrongFormatFile();
        Assert.assertEquals(2, failedFilesMsg.size());
        Assert.assertEquals("exception_file.sql", failedFilesMsg.get(0));
        Assert.assertEquals("error_file.sql", failedFilesMsg.get(1));
        // import empty file
        MockMultipartFile emptyFile = new MockMultipartFile("empty_file.sql", "empty_file.sql", "text/plain",
                "".getBytes(StandardCharsets.UTF_8));
        result = favoriteRuleService.importSqls(new MultipartFile[] { emptyFile }, PROJECT);
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.getSize());
    }

    @Test
    public void testTransformFileToSqls() throws IOException {
        List<String> sqls1 = favoriteRuleService.transformFileToSqls(
                new MockMultipartFile("sqls5.sql", "sqls5.sql", "text/plain",
                        Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls5.sql").toPath())),
                PROJECT);
        Assert.assertEquals(5, sqls1.size());
        Assert.assertEquals("select CAL_DT from TEST_KYLIN_FACT", sqls1.get(0));
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME), '123', 234, 'abc' from TEST_KYLIN_FACT",
                sqls1.get(1));
        Assert.assertEquals("select concat() from TEST_KYLIN_FACT", sqls1.get(2));
        Assert.assertEquals("select '456', 456, 'dgf' from TEST_KYLIN_FACT", sqls1.get(4));

        List<String> sqls2 = favoriteRuleService.transformFileToSqls(
                new MockMultipartFile("sqls6.sql", "sqls6.sql", "text/plain",
                        Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls6.sql").toPath())),
                PROJECT);
        Assert.assertEquals(1, sqls2.size());
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME) from TEST_KYLIN_FACT", sqls2.get(0));

        List<String> sqls3 = favoriteRuleService.transformFileToSqls(
                new MockMultipartFile("sqls7.sql", "sqls7.sql", "text/plain",
                        Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls7.sql").toPath())),
                PROJECT);
        Assert.assertEquals(2, sqls3.size());
        Assert.assertEquals("select CAL_DT from TEST_KYLIN_FACT", sqls3.get(0));
        Assert.assertEquals("select concat(';', LSTG_FORMAT_NAME), '123', 234, 'abc' from TEST_KYLIN_FACT",
                sqls3.get(1));

        List<String> sqls4 = favoriteRuleService.transformFileToSqls(
                new MockMultipartFile("sqls8.sql", "sqls8.sql", "text/plain",
                        Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls8.sql").toPath())),
                PROJECT);
        Assert.assertEquals(360, sqls4.size());
    }

    @Test
    public void testReadSqlFileWithSqlHints() throws IOException {
        List<String> sqlList = favoriteRuleService.transformFileToSqls(
                new MockMultipartFile("hint.sql", "hint.sql", "text/plain",
                        Files.newInputStream(new File("./src/test/resources/ut_sqls_file/hint.sql").toPath())),
                PROJECT);
        Assert.assertEquals(2, sqlList.size());
        Assert.assertEquals("--  CubePriority(m)\nselect price from test_kylin_fact", sqlList.get(0));
        Assert.assertEquals("select /*+ MODEL_PRIORITY(m) */ price from test_kylin_fact", sqlList.get(1));
    }

    @Test
    public void testSqlValidate() {
        {
            String sql = "select * from test_kylin_fact\n\n";
            var response = favoriteRuleService.sqlValidate(PROJECT, sql);
            Assert.assertTrue(response.isCapable());
        }

        {
            String sql = "select concat() from TEST_KYLIN_FACT";
            SQLValidateResponse response = favoriteRuleService.sqlValidate(PROJECT, sql);
            Assert.assertTrue(response.isCapable());
        }
    }

    @Test
    public void testSqlValidateError() {
        String sql = "select * from test_kylin\n\n";
        var response = favoriteRuleService.sqlValidate(PROJECT, sql);
        Assert.assertFalse(response.isCapable());
        Assert.assertTrue(Lists.newArrayList(response.getSqlAdvices()).get(0).getIncapableReason()
                .contains("Can’t find table \"TEST_KYLIN\". Please check and try again."));
    }

    @Test
    public void testImportCCSQLs() {
        val ccDesc = new ComputedColumnDesc();
        ccDesc.setTableAlias("TEST_KYLIN_FACT");
        ccDesc.setTableIdentity("DEFAULT.TEST_KYLIN_FACT");
        ccDesc.setColumnName("DEAL_AMOUNT");
        ccDesc.setDatatype("DECIMAL(30,4)");
        ccDesc.setExpression("TEST_KYLIN_FACT.PRICE * TEST_KYLIN_FACT.ITEM_COUNT");

        val basicModel = NDataModelManager.getInstance(getTestConfig(), PROJECT)
                .getDataModelDescByAlias("nmodel_basic");
        Assert.assertTrue(basicModel.getComputedColumnDescs().contains(ccDesc));

        // PRICE * ITEM_COUNT expression already exists
        String sql = "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT";

        MockMultipartFile file1 = new MockMultipartFile("file.sql", "file.sql", "text/plain",
                sql.getBytes(StandardCharsets.UTF_8));
        SQLParserResponse result = favoriteRuleService.importSqls(new MultipartFile[] { file1 }, PROJECT);
        List<ImportSqlResponse> responses = result.getData();
        Assert.assertEquals(1, responses.size());
        Assert.assertTrue(responses.get(0).isCapable());
        Assert.assertEquals(1, result.getSize());
        Assert.assertEquals(1, result.getCapableSqlNum());

        // same cc expression not replaced with existed cc
        Assert.assertEquals(sql, responses.get(0).getSql());
    }

    @Test
    public void testImportSqlExceedsLimit() throws Exception {
        MockMultipartFile file1 = new MockMultipartFile("sqls1.sql", "sqls1.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls1.sql").toPath()));
        MockMultipartFile file2 = new MockMultipartFile("sqls2.txt", "sqls2.txt", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls2.txt").toPath()));
        MockMultipartFile file4 = new MockMultipartFile("sqls4.sql", "sqls4.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls4.sql").toPath()));

        var response = favoriteRuleService.importSqls(new MultipartFile[] { file4 }, PROJECT);
        Assert.assertEquals(1200, response.getSize());

        response = favoriteRuleService.importSqls(new MultipartFile[] { file1, file2, file4 }, PROJECT);
        Assert.assertEquals(1210, response.getSize());
    }

    @Test
    public void testLoadStreamingSqls() throws IOException {
        MockMultipartFile file1 = new MockMultipartFile("sqls9.sql", "sqls9.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls9.sql").toPath()));
        MockMultipartFile file2 = new MockMultipartFile("sqls10.sql", "sqls10.sql", "text/plain",
                Files.newInputStream(new File("./src/test/resources/ut_sqls_file/sqls10.sql").toPath()));
        val response = favoriteRuleService.importSqls(new MultipartFile[] { file1, file2 }, "streaming_test");
        Assert.assertEquals(2, response.getSize());
        Assert.assertEquals(1, response.getCapableSqlNum());

    }

    @Test
    public void testSqlValidateFormatError() {
        String sql = "select * from \"test_kylin_fact\n\n";
        Assert.assertThrows(KylinException.class, () -> favoriteRuleService.sqlValidate(PROJECT, sql));
    }
}
