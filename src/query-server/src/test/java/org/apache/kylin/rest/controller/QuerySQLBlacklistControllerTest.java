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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.blacklist.SQLBlacklist;
import org.apache.kylin.query.blacklist.SQLBlacklistItem;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.apache.kylin.rest.service.QuerySQLBlacklistService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class QuerySQLBlacklistControllerTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private MockMvc mockMvc;
    @Mock
    private QuerySQLBlacklistService querySQLBlacklistService;
    @InjectMocks
    private QuerySQLBlacklistController querySQLBlacklistController = Mockito.spy(new QuerySQLBlacklistController());

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        mockMvc = MockMvcBuilders.standaloneSetup(querySQLBlacklistController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        when(querySQLBlacklistService.getSqlBlacklist(PROJECT)).thenReturn(sqlBlacklist);
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.get("/api/query_sql_blacklist/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON).param("project", PROJECT)
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(querySQLBlacklistController).getSqlBlacklist(PROJECT);
    }

    @Test
    public void testAddItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest request = new SQLBlacklistItemRequest();
        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/add_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException) mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028003", errorCode.getCodeString());

        request.setSql(sql);
        request.setRegex(".*");
        request.setConcurrentLimit(0);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/add_item/{project}", PROJECT)
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    @Test
    public void testOverwrite() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest sqlBlacklistItemRequest = new SQLBlacklistItemRequest();
        sqlBlacklistItemRequest.setSql(sql);
        sqlBlacklistItemRequest.setRegex(".*");
        sqlBlacklistItemRequest.setConcurrentLimit(0);

        SQLBlacklistRequest request = new SQLBlacklistRequest();
        request.setBlacklistItems(Lists.newArrayList(sqlBlacklistItemRequest));

        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/overwrite")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException) mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028004", errorCode.getCodeString());

        request.setProject(PROJECT);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/overwrite")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
    }

    @Test
    public void testUpdateItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        SQLBlacklistItemRequest sqlBlacklistItemRequest = new SQLBlacklistItemRequest();

        MvcResult mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/update_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlBlacklistItemRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        ErrorCode errorCode = ((KylinException) mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028007", errorCode.getCodeString());

        sqlBlacklistItemRequest.setId("5b7bcee9-e22f-beb6-b14d-4f8ce01b1446");
        mvcResult = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/query_sql_blacklist/update_item/{project}", PROJECT)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(JsonUtil.writeValueAsString(sqlBlacklistItemRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertTrue(mvcResult.getResolvedException() instanceof KylinException);
        errorCode = ((KylinException) mvcResult.getResolvedException()).getErrorCode();
        Assert.assertEquals("KE-010028003", errorCode.getCodeString());

        sqlBlacklistItemRequest.setSql(sql);
        sqlBlacklistItemRequest.setRegex(".*");
        sqlBlacklistItemRequest.setConcurrentLimit(0);
    }

    @Test
    public void testDeleteItem() throws Exception {
        String sql = "select count(*) from TEST_KYLIN_FACT";
        SQLBlacklist sqlBlacklist = new SQLBlacklist();
        SQLBlacklistItem sqlBlacklistItem = new SQLBlacklistItem();
        sqlBlacklistItem.setSql(sql);
        sqlBlacklistItem.setRegex(".*");
        sqlBlacklistItem.setConcurrentLimit(0);

        sqlBlacklist.setProject(PROJECT);
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);

        when(querySQLBlacklistService.deleteSqlBlacklistItem(PROJECT, "0")).thenReturn(sqlBlacklist);

        MvcResult mvcResult = mockMvc.perform(MockMvcRequestBuilders
                .delete("/api/query_sql_blacklist/delete_item/{project}/{id}", PROJECT, "0")
                .contentType(MediaType.APPLICATION_JSON).accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(querySQLBlacklistController).deleteItem(PROJECT, "0");
    }
}
