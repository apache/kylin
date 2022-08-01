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
package org.apache.kylin.sdk.datasource;

import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.sdk.datasource.framework.JdbcConnectorTest;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class PushdownRunnerSDKImplTest extends JdbcConnectorTest {
    @Test
    public void testExecuteQuery() throws Exception {
        NProjectManager npr = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = npr.getProject("default");
        projectInstance.setDefaultDatabase("SSB");
        npr.updateProject(projectInstance);

        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        pushDownRunnerSDK.init(getTestConfig());
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();
        String sql = "select count(*) from LINEORDER";
        pushDownRunnerSDK.executeQuery(sql, returnRows, returnColumnMeta, "default");
        Assert.assertEquals("6005", returnRows.get(0).get(0));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        pushDownRunnerSDK.init(getTestConfig());
        String sql = "update SSB.LINEORDER set LO_TAX=1 where LO_ORDERKEY = 1";
        pushDownRunnerSDK.executeUpdate(sql, null);
    }

    @Test
    public void testGetName() {
        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        Assert.assertEquals(QueryContext.PUSHDOWN_RDBMS, pushDownRunnerSDK.getName());
    }
}
