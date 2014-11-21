/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.controller;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.kylinolap.rest.service.AdminService;
import com.kylinolap.rest.service.CubeService;
import com.kylinolap.rest.service.ServiceTestBase;

/**
 * @author xduo
 * 
 */
public class AdminControllerTest extends ServiceTestBase {

    private AdminController adminController;

    @Autowired
    private AdminService adminService;
    @Autowired
    private CubeService cubeService;

    @Before
    public void setup() {
        super.setUp();
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
