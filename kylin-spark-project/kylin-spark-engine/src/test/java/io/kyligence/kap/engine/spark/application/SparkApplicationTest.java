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

package io.kyligence.kap.engine.spark.application;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.mockito.Mockito;

import io.kyligence.kap.engine.spark.NSparkBasicTest;

public class SparkApplicationTest extends NSparkBasicTest {
    File tempDir = new File("./temp/");
    File file1 = new File(tempDir, "temp1_" + ResourceDetectUtils.fileName());
    File file2 = new File(tempDir, "temp2_" + ResourceDetectUtils.fileName());
    File sourceFile1 = new File(tempDir, "source1.txt");
    File sourceFile2 = new File(tempDir, "source2.txt");

    @Before
    public void before() throws IOException {
        FileUtils.forceMkdir(tempDir);

    }

    @After
    public void after() {
        FileUtils.deleteQuietly(tempDir);
    }

    @Test
    public void testChooseContentSize() throws Exception {
        SparkApplication application = new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        };

        // write source file
        FileOutputStream out1 = new FileOutputStream(sourceFile1);
        String minString = "test";
        out1.write(minString.getBytes());
        out1.close();

        FileOutputStream out2 = new FileOutputStream(sourceFile2);
        String maxString = "test_test";
        out2.write(maxString.getBytes());
        out2.close();

        // write resource_path file
        Map<String, List<String>> map1 = Maps.newHashMap();
        map1.put("1", Lists.newArrayList(sourceFile1.getAbsolutePath()));
        ResourceDetectUtils.write(new Path(file1.getAbsolutePath()), map1);

        Map<String, List<String>> map2 = Maps.newHashMap();
        map2.put("1", Lists.newArrayList(sourceFile2.getAbsolutePath()));
        ResourceDetectUtils.write(new Path(file2.getAbsolutePath()), map2);

        Assert.assertEquals(maxString.getBytes().length + "b",
                application.chooseContentSize(new Path(tempDir.getAbsolutePath())));
    }

    @Test
    public void testUpdateSparkJobExtraInfo() throws Exception {
        System.setProperty("spark.driver.param.taskId", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");
        SparkApplication application = Mockito.spy(new SparkApplication() {
            @Override
            protected void doExecute() throws Exception {
                System.out.println("empty");
            }
        });

        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/")
                .when(application).getTrackingUrl("application_1561370224051_0160");

        Map<String, String> payload = new HashMap<>(5);
        payload.put("project", "test_job_output");
        payload.put("jobId", "cb91189b-2b12-4527-aa35-0130e7d54ec0");
        payload.put("taskId", "cb91189b-2b12-4527-aa35-0130e7d54ec0_01");
        payload.put("yarnAppId", "application_1561370224051_0160");
        payload.put("yarnAppUrl", "http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/");

        String payloadJson = new ObjectMapper().writeValueAsString(payload);
        Mockito.doReturn(Boolean.TRUE).when(application).updateSparkJobInfo(payloadJson);

        Assert.assertTrue(application.updateSparkJobExtraInfo("test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", "application_1561370224051_0160"));

        Mockito.verify(application).getTrackingUrl("application_1561370224051_0160");
        Mockito.verify(application).updateSparkJobInfo(payloadJson);

        Mockito.reset(application);
        Mockito.doReturn("http://sandbox.hortonworks.com:8088/proxy/application_1561370224051_0160/")
                .when(application).getTrackingUrl("application_1561370224051_0160");
        Mockito.doReturn(Boolean.FALSE).when(application).updateSparkJobInfo(payloadJson);
        Assert.assertFalse(application.updateSparkJobExtraInfo("test_job_output",
                "cb91189b-2b12-4527-aa35-0130e7d54ec0", "application_1561370224051_0160"));

        Mockito.verify(application).getTrackingUrl("application_1561370224051_0160");
        Mockito.verify(application, Mockito.times(3)).updateSparkJobInfo(payloadJson);
    }
}
