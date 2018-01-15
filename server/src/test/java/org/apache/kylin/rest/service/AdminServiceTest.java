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
        
        String expected = "kylin.web.link-streaming-guide=http://kylin.apache.org/\n" +
                "kylin.web.contact-mail=\n" +
                "kylin.query.cache-enabled=true\n" +
                "kylin.web.link-diagnostic=\n" +
                "kylin.web.help.length=4\n" +
                "kylin.web.timezone=GMT+8\n" +
                "kylin.server.external-acl-provider=\n" +
                "kylin.storage.default=2\n" +
                "kylin.web.help=\n" +
                "kylin.web.export-allow-other=true\n" +
                "kylin.web.link-hadoop=\n" +
                "kylin.web.hide-measures=RAW\n" +
                "kylin.htrace.show-gui-trace-toggle=false\n" +
                "kylin.web.export-allow-admin=true\n" +
                "kylin.env=QA\n" +
                "kylin.web.hive-limit=20\n" +
                "kylin.engine.default=2\n" +
                "kylin.web.help.3=onboard|Cube Design Tutorial|\n" +
                "kylin.web.help.2=tableau|Tableau Guide|\n" +
                "kylin.web.help.1=odbc|ODBC Driver|\n" +
                "kylin.web.help.0=start|Getting Started|\n" +
                "kylin.security.profile=testing\n";
        Assert.assertEquals(expected, adminService.getPublicConfig());
    }
}
