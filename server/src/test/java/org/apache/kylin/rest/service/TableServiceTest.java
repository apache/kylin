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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class TableServiceTest extends ServiceTestBase {
    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Test
    public void testLoadHiveTablesToProject() throws Exception {
        TableMetadataManager tableMgr = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        TableDesc tableDesc = tableMgr.getTableDesc("TEST_KYLIN_FACT", "default");
        TableExtDesc tableExt = tableMgr.getTableExt(tableDesc);
        String[] defaults = tableService.loadTableToProject(tableDesc, tableExt, "default");
        Assert.assertEquals("DEFAULT.TEST_KYLIN_FACT", defaults[0]);
    }
}
