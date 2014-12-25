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

package com.kylinolap.rest.service;

import com.kylinolap.dict.DictionaryManager;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.metadata.project.ProjectManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.kylinolap.common.util.HBaseMiniclusterMetadataTestCase;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.metadata.MetadataManager;

/**
 * @author xduo
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:applicationContext.xml", "classpath:kylinSecurity.xml" })
@ActiveProfiles("testing")
public class ServiceTestBase extends HBaseMiniclusterMetadataTestCase { //HBaseMetadataTestCase {

    @BeforeClass
    public static void setupResource() throws Exception {
        startupMinicluster();

        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", "ROLE_ADMIN");
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void tearDownResource() {
        shutdownMiniCluster();
    }


    @Before
    public void setUp() {
        this.createTestMetadata();

        MetadataManager.removeInstance(this.getTestConfig());
        DictionaryManager.removeInstance(this.getTestConfig());
        CubeManager.removeInstance(this.getTestConfig());
        IIManager.removeInstance(this.getTestConfig());
        ProjectManager.removeInstance(this.getTestConfig());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    /**
     * better keep this method, otherwise cause error
     * com.kylinolap.rest.service.TestBase.initializationError
     */
    @Test
    public void test() {
    }
}
