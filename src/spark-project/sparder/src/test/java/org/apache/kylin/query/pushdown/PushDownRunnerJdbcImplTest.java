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

package org.apache.kylin.query.pushdown;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.List;

public class PushDownRunnerJdbcImplTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata(Lists.newArrayList(overlay));
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            // ignore it
        }
        cleanSingletonInstances();
    }

    private static void cleanSingletonInstances() {
        try {
            getInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getGlobalInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstancesFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProjectFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProject().clear();
        } catch (Exception e) {
            //ignore in it
        }
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
        val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
        overwriteSystemProp("KYLIN_HOME", kylinHomePath);
        val jobJar = org.apache.kylin.common.util.FileUtils.findFile(
                new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "kap-assembly(.?)\\.jar");
        getTestConfig().setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    @Test
    public void testPushdownJdbc() throws Exception {
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        NProjectManager npr = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = npr.getProject("default");
        projectInstance.setDefaultDatabase("SSB");
        LinkedHashMap<String, String> overrideKylinProps = Maps.newLinkedHashMap();
        overrideKylinProps.put("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.username", "sa");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.password", "");
        projectInstance.setOverrideKylinProps(overrideKylinProps);
        npr.updateProject(projectInstance);
        KylinConfigExt config = projectInstance.getConfig();
        PushDownRunnerJdbcImpl pushDownRunnerJdbc = new PushDownRunnerJdbcImpl();
        pushDownRunnerJdbc.init(config);
        String sql = "select 1";
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();
        pushDownRunnerJdbc.executeQuery(sql, returnRows, returnColumnMeta, "default");
        Assert.assertEquals("1", returnRows.get(0).get(0));
    }
}
