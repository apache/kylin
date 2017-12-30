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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * 
 */
public class AdminServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("adminService")
    private AdminService adminService;

    @Test
    public void testGetPublicConfig() throws IOException {
        //set ../examples/test_metadata/kylin.properties empty
        File file = FileUtils.getFile(LOCALMETA_TEMP_DATA + "kylin.properties");
        FileUtils.deleteQuietly(file);
        FileUtils.touch(file);
        String path = Thread.currentThread().getContextClassLoader().getResource("kylin.properties").getPath();
        KylinConfig.setKylinConfigThreadLocal(KylinConfig.createInstanceFromUri(path));
        
        String publicConfig = adminService.getPublicConfig();
        
        Assert.assertFalse(publicConfig.contains("kylin.metadata.data-model-manager-impl"));
        Assert.assertFalse(publicConfig.contains("kylin.dictionary.use-forest-trie"));
        Assert.assertFalse(publicConfig.contains("kylin.cube.segment-advisor"));
        Assert.assertFalse(publicConfig.contains("kylin.job.use-remote-cli"));
        Assert.assertFalse(publicConfig.contains("kylin.job.scheduler.provider"));
        Assert.assertFalse(publicConfig.contains("kylin.engine.mr.job-jar"));
        Assert.assertFalse(publicConfig.contains("kylin.engine.spark.sanity-check-enabled"));
        Assert.assertFalse(publicConfig.contains("kylin.storage.provider"));
        Assert.assertFalse(publicConfig.contains("kylin.query.convert-create-table-to-with"));
        Assert.assertFalse(publicConfig.contains("kylin.server.init-tasks"));
    }
}
