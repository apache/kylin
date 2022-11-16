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
package org.apache.kylin.common.metric;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsController;
import org.apache.kylin.common.metrics.MetricsInfluxdbReporter;
import org.apache.kylin.common.metrics.service.InfluxDBInstance;
import org.apache.kylin.common.util.InfluxDBUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.kyligence.kap.shaded.influxdb.org.influxdb.InfluxDB;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*", "javax.script.*"})
@PrepareForTest({InfluxDBInstance.class, InfluxDBUtils.class, UserGroupInformation.class})
public class MetricsControllerTest extends NLocalFileMetadataTestCase{
    public final static String ROLE_ADMIN = "ROLE_ADMIN";
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", ROLE_ADMIN);


    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(UserGroupInformation.class);
        PowerMockito.mockStatic(InfluxDBUtils.class);
        
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenAnswer(invocation -> userGroupInformation);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }


    @Test
    public void initTest() throws Exception {
        KapConfig config = KapConfig.wrap(KylinConfig.getInstanceFromEnv());

        InfluxDB influxDB = Mockito.mock(InfluxDB.class);
        PowerMockito.doAnswer(invocationOnMock -> influxDB).when(InfluxDBUtils.class, "getInfluxDBInstance", Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyBoolean());
        Mockito.when(influxDB.databaseExists(Mockito.anyString())).thenReturn(true);

        MetricsController.init(config);

        MetricsInfluxdbReporter influxdbReporter = MetricsInfluxdbReporter.getInstance();
        influxdbReporter.start(config);
        Assert.assertTrue(influxdbReporter.isRunning());
    }

    @Test
    public void startTest() {
        KapConfig config = KapConfig.wrap(KylinConfig.getInstanceFromEnv());

        MetricsInfluxdbReporter influxdbReporter = MetricsInfluxdbReporter.getInstance();
        influxdbReporter.start(config);
        Assert.assertFalse(influxdbReporter.isRunning());
    }

}