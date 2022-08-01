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
package org.apache.kylin.tool.obf;

import java.io.File;
import java.nio.file.Files;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class KylinConfObfuscatorTest extends NLocalFileMetadataTestCase {

    public static void prepare() throws Exception {
        val confFile = new File(
                KylinConfObfuscatorTest.class.getClassLoader().getResource("obf/kylin.properties").getFile());
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        confPath.mkdirs();
        File kylinConf = new File(confPath, "kylin.properties");
        Files.copy(confFile.toPath(), kylinConf.toPath());
    }

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testObfKylinConf() throws Exception {
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        confPath.mkdirs();
        File kylinConf = new File(confPath, "kylin.properties");
        File outputFile = new File(path, "output.json");
        ResultRecorder resultRecorder = new ResultRecorder();
        try (MappingRecorder mappingRecorder = new MappingRecorder(outputFile)) {
            FileObfuscator fileObfuscator = new KylinConfObfuscator(ObfLevel.OBF, mappingRecorder, resultRecorder);
            fileObfuscator.obfuscateFile(kylinConf);
        }
        val properties = FileUtils.readFromPropertiesFile(kylinConf);
        Assert.assertEquals(
                "zjz@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://10.1.3.16:3306/kylin,username=<hidden>,password=<hidden>,maxActive=20,maxIdle=20,test=",
                properties.get("kylin.metadata.url"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.env.zookeeper-connect-string"));
        Assert.assertEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.influxdb.password"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.influxdb.address"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.metrics.influx-rpc-service-bind-address"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("server.port"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.metadata.random-admin-password.enabled"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.security.user-password-encoder"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.job.notification-admin-emails"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN, properties.get("kylin.job.tracking-url-pattern"));
        Assert.assertNotEquals(SensitiveConfigKeysConstant.HIDDEN,
                properties.get("kylin.storage.columnar.spark-conf.spark.executor.cores"));

    }

    @Test
    public void testRawKylinConf() throws Exception {
        val url = KylinConfig.getInstanceFromEnv().getMetadataUrl();
        File path = new File(url.getIdentifier());
        File confPath = new File(path.getParentFile(), "conf");
        File kylinConf = new File(confPath, "kylin.properties");
        File outputFile = new File(path, "output.json");
        ResultRecorder resultRecorder = new ResultRecorder();
        try (MappingRecorder mappingRecorder = new MappingRecorder(outputFile)) {
            FileObfuscator fileObfuscator = new KylinConfObfuscator(ObfLevel.RAW, mappingRecorder, resultRecorder);
            fileObfuscator.obfuscateFile(kylinConf);
        }
        val properties = FileUtils.readFromPropertiesFile(kylinConf);
        for (val kv : properties.entrySet()) {
            Assert.assertFalse(kv.getValue().contains(SensitiveConfigKeysConstant.HIDDEN));
        }
    }
}
