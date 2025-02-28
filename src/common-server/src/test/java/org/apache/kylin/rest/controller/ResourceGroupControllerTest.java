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

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PARAMETER_IN_PARAMETER_NOT_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REPEATED_INSTANCE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_BINDING_PROJECT_INVALID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_DISABLE_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ENABLE_FAILED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_ALREADY_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_ID_NOT_FOUND_IN_INSTANCE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.RESOURCE_GROUP_INCOMPLETE_PARAMETER_LIST;

import java.util.List;
import java.util.Map;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.ResourceGroup;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupEntity;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupMappingInfo;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.handler.resourcegroup.IResourceGroupRequestValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupDisabledValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupEnabledValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupEntityValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupFieldValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupKylinInstanceValidator;
import org.apache.kylin.rest.handler.resourcegroup.ResourceGroupMappingInfoValidator;
import org.apache.kylin.rest.request.resourecegroup.ResourceGroupRequest;
import org.apache.kylin.rest.service.ResourceGroupService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import com.google.gson.Gson;

import lombok.val;

public class ResourceGroupControllerTest extends NLocalFileMetadataTestCase {
    private MockMvc mockMvc;

    @Spy
    private List<IResourceGroupRequestValidator> requestFilterList = Lists.newArrayList();

    @Mock
    private ResourceGroupService resourceGroupService;

    @InjectMocks
    private ResourceGroupController resourceGroupController = Mockito.spy(new ResourceGroupController());

    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(resourceGroupController)
                .defaultRequest(MockMvcRequestBuilders.get("/")).build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
        requestFilterList.add(new ResourceGroupFieldValidator());
        requestFilterList.add(new ResourceGroupDisabledValidator());
        requestFilterList.add(new ResourceGroupEnabledValidator());
        requestFilterList.add(new ResourceGroupEntityValidator());
        requestFilterList.add(new ResourceGroupKylinInstanceValidator());
        requestFilterList.add(new ResourceGroupMappingInfoValidator());
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    //=============  ResourceGroupFieldValidator  ===============

    @Test
    public void testResourceGroupFieldFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_INCOMPLETE_PARAMETER_LIST.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupFieldFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();
        Assert.assertThrows(RESOURCE_GROUP_INCOMPLETE_PARAMETER_LIST.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupFieldFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_INCOMPLETE_PARAMETER_LIST.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupFieldFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_INCOMPLETE_PARAMETER_LIST.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  ResourceGroupDisabledValidator  ===============

    @Test
    public void testResourceGroupDisabledFilter1() throws Exception {
        setResourceGroupEnabled();

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilter2() throws Exception {
        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(false);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());
        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();
        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController).updateResourceGroup(request);
    }

    @Test
    public void testResourceGroupDisabledFilterException2() throws Exception {
        setResourceGroupEnabled();

        ResourceGroupRequest request = new ResourceGroupRequest();
        List<KylinInstance> kylinInstances = Lists.newArrayList(new KylinInstance(), new KylinInstance());
        request.setKylinInstances(kylinInstances);
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_DISABLE_FAILED.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  ResourceGroupEnabledValidator  ===============

    @Test
    public void testResourceGroupEnabledFilterException1() throws Exception {
        setResourceGroupEnabled();

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());
        request.setResourceGroupEntities(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_ENABLE_FAILED.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  ResourceGroupEntityValidator  ===============

    @Test
    public void testResourceGroupEntityFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        request.setResourceGroupEntities(Lists.newArrayList(new ResourceGroupEntity(), new ResourceGroupEntity()));
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_ID_EMPTY.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupEntityFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);
        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");

        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);

        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        request.setKylinInstances(Lists.newArrayList());
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_ID_ALREADY_EXIST.getMsg("123"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  ResourceGroupEntityValidator  ===============

    @Test
    public void testResourceGroupKylinInstanceFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);

        request.setResourceGroupEntities(Lists.newArrayList(entity1));
        request.setKylinInstances(Lists.newArrayList(new KylinInstance()));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("instance"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY.getMsg("resource_group_id"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "1");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_ID_NOT_FOUND_IN_INSTANCE.getMsg("1"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupKylinInstanceFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");

        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance, instance));
        request.setResourceGroupMappingInfoList(Lists.newArrayList());

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(REPEATED_INSTANCE.getMsg(), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  ResourceGroupMappingInfoValidator  ===============

    @Test
    public void testResourceGroupMappingInfoFilterException1() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        request.setResourceGroupMappingInfoList(Lists.newArrayList(new ResourceGroupMappingInfo()));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(PARAMETER_IN_PARAMETER_NOT_EMPTY.getMsg("project", "mapping_info"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupMappingInfoFilterException2() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "213");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(PROJECT_NOT_EXIST.getMsg("213"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupMappingInfoFilterException3() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(PARAMETER_IN_PARAMETER_NOT_EMPTY.getMsg("resource_group_id", "mapping_info"),
                KylinException.class, () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupMappingInfoFilterException4() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "1");
        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO.getMsg("1"), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupMappingInfoFilterException5() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("id", "124");
        Map<String, String> map3 = Maps.newHashMap();
        map3.put("id", "125");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupEntity.class);
        val entity3 = JsonUtil.readValue(new Gson().toJson(map3), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2, entity3));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "123");
        map.put("request_type", "BUILD");

        map2.clear();
        map2.put("project", "default");
        map2.put("resource_group_id", "124");
        map2.put("request_type", "QUERY");

        map3.clear();
        map3.put("project", "default");
        map3.put("resource_group_id", "125");
        map3.put("request_type", "QUERY");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        val mapping3 = JsonUtil.readValue(new Gson().toJson(map3), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2, mapping3));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_BINDING_PROJECT_INVALID.getMsg(map.get("project")), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    @Test
    public void testResourceGroupMappingInfoFilterException6() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("id", "124");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "123");
        map.put("request_type", "BUILD");

        map2.clear();
        map2.put("project", "default");
        map2.put("resource_group_id", "124");
        map2.put("request_type", "BUILD");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2));

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isInternalServerError()).andReturn();

        Assert.assertThrows(RESOURCE_GROUP_BINDING_PROJECT_INVALID.getMsg(map.get("project")), KylinException.class,
                () -> resourceGroupController.updateResourceGroup(request));
    }

    //=============  pass case  ===============

    @Test
    public void testPassCase() throws Exception {
        ResourceGroupRequest request = new ResourceGroupRequest();
        request.setResourceGroupEnabled(true);

        Map<String, String> map = Maps.newHashMap();
        map.put("id", "123");
        Map<String, String> map2 = Maps.newHashMap();
        map2.put("id", "124");
        val entity1 = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupEntity.class);
        val entity2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupEntity.class);
        request.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));

        map.clear();
        map.put("instance", "1.1.1.1:7070");
        map.put("resource_group_id", "123");
        val instance = JsonUtil.readValue(new Gson().toJson(map), KylinInstance.class);
        request.setKylinInstances(Lists.newArrayList(instance));

        map.clear();
        map.put("project", "default");
        map.put("resource_group_id", "123");
        map.put("request_type", "BUILD");

        map2.clear();
        map2.put("project", "default");
        map2.put("resource_group_id", "124");
        map2.put("request_type", "QUERY");

        val mapping = JsonUtil.readValue(new Gson().toJson(map), ResourceGroupMappingInfo.class);
        val mapping2 = JsonUtil.readValue(new Gson().toJson(map2), ResourceGroupMappingInfo.class);
        request.setResourceGroupMappingInfoList(Lists.newArrayList(mapping, mapping2));

        Mockito.doReturn(Mockito.mock(ResourceGroup.class)).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1));
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        resourceGroup.setKylinInstances(Lists.newArrayList());
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        resourceGroup.setResourceGroupEnabled(true);
        resourceGroup.setResourceGroupEntities(Lists.newArrayList(entity1, entity2));
        resourceGroup.setKylinInstances(Lists.newArrayList(instance));
        resourceGroup.setResourceGroupMappingInfoList(Lists.newArrayList(mapping));
        Mockito.doReturn(resourceGroup).when(resourceGroupService).getResourceGroup();

        mockMvc.perform(MockMvcRequestBuilders.put("/api/resource_groups").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(request))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();

        Mockito.verify(resourceGroupController, Mockito.times(4)).updateResourceGroup(request);
    }

    //=============  methods  ===============

    private void setResourceGroupEnabled() {
        val manager = ResourceGroupManager.getInstance(getTestConfig());
        manager.getResourceGroup();
        manager.updateResourceGroup(copyForWrite -> copyForWrite.setResourceGroupEnabled(true));
    }

}
