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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.response.AccessEntryResponse;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xduo
 */
public class AccessControllerTest extends ServiceTestBase {

    private AccessController accessController;

    @Autowired
    AccessService accessService;

    @Before
    public void setup() throws Exception {
        super.setup();

        accessController = new AccessController();
        accessController.setAccessService(accessService);
    }

    @Test
    public void testBasics() throws IOException {
        List<AccessEntryResponse> aes = accessController.getAccessEntities("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92");
        Assert.assertTrue(aes.size() == 0);

        AccessRequest accessRequest = new AccessRequest();
        accessRequest.setPermission("ADMINISTRATION");
        accessRequest.setSid("MODELER");
        accessRequest.setPrincipal(true);

        aes = accessController.grant("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertTrue(aes.size() == 1);

        Long aeId = null;
        for (AccessEntryResponse ae : aes) {
            aeId = (Long) ae.getId();
        }
        Assert.assertNotNull(aeId);

        accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(aeId);
        accessRequest.setPermission("READ");

        aes = accessController.update("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertTrue(aes.size() == 1);
        for (AccessEntryResponse ae : aes) {
            aeId = (Long) ae.getId();
        }
        Assert.assertNotNull(aeId);

        accessRequest = new AccessRequest();
        accessRequest.setAccessEntryId(aeId);
        accessRequest.setPermission("READ");
        aes = accessController.revoke("CubeInstance", "a24ca905-1fc6-4f67-985c-38fa5aeafd92", accessRequest);
        Assert.assertEquals(0, aes.size());
    }
}
