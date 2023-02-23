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

package org.apache.kylin.server;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.request.ComputedColumnCheckRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.junit.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

public class NModelControllerWithRealServiceTest extends AbstractMVCIntegrationTestCase {

    @Test
    public void testCheckComputedColumns() throws Exception {

        final ComputedColumnCheckRequest ccRequest = new ComputedColumnCheckRequest();
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), "default");
        NDataModel model = modelManager
                .copyForWrite(modelManager.getDataModelDesc("741ca86a-1f13-46da-a59f-95fb68615e3a"));
        model.getComputedColumnDescs().get(0).setColumnName("rename_cc");
        ModelRequest modelRequest = new ModelRequest(model);
        modelRequest.setProject("default");
        ccRequest.setModelDesc(modelRequest);
        ccRequest.setProject("default");

        final MvcResult result = mockMvc
                .perform(MockMvcRequestBuilders.post("/api/models/computed_columns/check")
                        .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(ccRequest))
                        .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().is5xxServerError()).andReturn();
    }
}
