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

import org.apache.kylin.rest.service.AdminService;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * @author xduo
 */
public class AdminControllerTest extends ServiceTestBase {

    private AdminController adminController;

    @Autowired
    @Qualifier("adminService")
    private AdminService adminService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Before
    public void setup() throws Exception {
        super.setup();
        adminController = new AdminController();
        adminController.setAdminService(adminService);
        adminController.setCubeMgmtService(cubeService);
    }

    @Test
    public void testBasics() throws IOException {
        Assert.assertNotNull(adminController.getConfig());
        Assert.assertNotNull(adminController.getEnv());
    }
}
