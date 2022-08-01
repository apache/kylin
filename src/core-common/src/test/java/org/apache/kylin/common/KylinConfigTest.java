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

package org.apache.kylin.common;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

import lombok.val;

@MetadataInfo(onlyProps = true)
public class KylinConfigTest {

    @BeforeEach
    public void setup() {
        val config = getTestConfig();
        config.setProperty("kylin.test.bcc.new-key", "some-value");
        config.setProperty("kylin.engine.mr.config-override.test1", "test1");
        config.setProperty("kylin.engine.mr.config-override.test2", "test2");
        config.setProperty("kylin.job.lock", "org.apache.kylin.job.lock.MockJobLock");
        config.setProperty("kap.storage.columnar.spark-conf.spark.driver.memory", "1234m");
        config.setProperty("kap.storage.columnar.spark-conf.spark.executor.memoryOverhead", "4321m");
        config.setProperty("kap.storage.monitor-spark-period-seconds", "5678");

    }

    @Test
    public void testDuplicateConfig() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String v = config.getJobControllerLock();
        Assert.assertEquals("org.apache.kylin.job.lock.MockJobLock", v);
    }

    @Test
    public void testBackwardCompatibility() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String oldk = "kylin.test.bcc.old-key";
        final String newk = "kylin.test.bcc.new-key";

        Assert.assertNull(config.getOptional(oldk));
        Assert.assertNotNull(config.getOptional(newk));

        Map<String, String> override = Maps.newHashMap();
        override.put(oldk, "1");
        KylinConfigExt ext = KylinConfigExt.createInstance(config, override);
        Assert.assertNull(ext.getOptional(oldk));
        Assert.assertEquals(ext.getOptional(newk), "1");
        Assert.assertNotEquals(config.getOptional(newk), "1");

        config.setProperty(oldk, "2");
        Assert.assertEquals(config.getOptional(newk), "2");
    }

    @Test
    public void testBackwardCompatibility_KAP_KYLIN() {
        Properties config = KylinConfig.getInstanceFromEnv().getAllProperties();
        Assert.assertEquals(config.get("kylin.storage.columnar.spark-conf.spark.driver.memory"), "1234m");
        Assert.assertEquals(config.get("kylin.storage.columnar.spark-conf.spark.executor.memoryOverhead"), "4321m");
        Assert.assertEquals(config.get("kylin.storage.monitor-spark-period-seconds"), "5678");
    }

    @Test
    public void testExtShareTheBase() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = Maps.newHashMap();
        KylinConfig configExt = KylinConfigExt.createInstance(config, override);
        Assert.assertSame(config.properties, configExt.properties);
        config.setProperty("1234", "1234");
        Assert.assertEquals("1234", configExt.getOptional("1234"));
    }

    @Test
    public void testGetMetadataUrlPrefix() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        config.setMetadataUrl("testMetaPrefix@hdfs");
        Assert.assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("/kylin/temp");
        Assert.assertEquals("/kylin/temp", config.getMetadataUrlPrefix());
    }

    @Test
    public void testThreadLocalOverride() throws InterruptedException {
        final String metadata1 = "meta1";
        final String metadata2 = "meta2";

        // set system KylinConfig
        KylinConfig sysConfig = KylinConfig.getInstanceFromEnv();
        sysConfig.setMetadataUrl(metadata1);

        Assert.assertEquals(metadata1, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

        // test thread-local override
        KylinConfig threadConfig = KylinConfig.createKylinConfig(new Properties());
        threadConfig.setMetadataUrl(metadata2);

        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(threadConfig)) {

            Assert.assertEquals(metadata2, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

            // other threads still use system KylinConfig
            final String[] holder = new String[1];
            Thread child = new Thread(new Runnable() {
                @Override
                public void run() {
                    holder[0] = KylinConfig.getInstanceFromEnv().getMetadataUrl().toString();
                }
            });
            child.start();
            child.join();
            Assert.assertEquals(metadata1, holder[0]);
        }
    }

    @Test
    public void testOverrideSparkJobJarPath() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String oldSparkJobJarPath = System.getProperty("kylin.engine.spark.job-jar");
        String overrideSparkJobJarPath = oldSparkJobJarPath + "_override";
        conf.overrideSparkJobJarPath(overrideSparkJobJarPath);
        String newSparkJobJarPath = System.getProperty("kylin.engine.spark.job-jar");
        Assert.assertEquals(newSparkJobJarPath, overrideSparkJobJarPath);

        if (StringUtils.isBlank(oldSparkJobJarPath)) {
            // remove property, otherwise org.apache.kylin.common.KylinConfigBase.getOptional(java.lang.String, java.lang.String)
            // will return empty str
            System.clearProperty("kylin.engine.spark.job-jar");
        } else {
            conf.overrideSparkJobJarPath(oldSparkJobJarPath);
        }
    }

    @Test
    public void testGetKylinJobJarPath() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String kylinJobJarPath = conf.getKylinJobJarPath();
        Assert.assertEquals(kylinJobJarPath, "");
    }

    @Test
    public void testPlaceholderReplace() {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        {
            kylinConfig.setProperty("ph_1", "${prop_a}/${prop1}");
            Assert.assertEquals("${prop_a}/${prop1}", kylinConfig.getOptional("ph_1"));

            kylinConfig.setProperty("prop_a", "prop_A");
            kylinConfig.setProperty("prop1", "prop_1");
            Assert.assertEquals("prop_A/prop_1", kylinConfig.getOptional("ph_1"));
        }

        {
            kylinConfig.setProperty("ph_2", "${prop2}/${prop_b}");
            Assert.assertEquals("${prop2}/${prop_b}", kylinConfig.getOptional("ph_2"));

            kylinConfig.setProperty("prop_b", "prop_B");
            kylinConfig.setProperty("prop2", "${prop_b}");
            Assert.assertEquals("prop_B/prop_B", kylinConfig.getOptional("ph_2"));
        }

        {
            kylinConfig.setProperty("ph_3", "${prop3}/${prop_c}/xxx/${prop2}/${prop_xxx}");
            Assert.assertEquals("${prop3}/${prop_c}/xxx/prop_B/${prop_xxx}", kylinConfig.getOptional("ph_3"));

            kylinConfig.setProperty("prop_c", "${prop_C}");
            kylinConfig.setProperty("prop3", "prop_3");
            Assert.assertEquals("prop_3/${prop_C}/xxx/prop_B/${prop_xxx}", kylinConfig.getOptional("ph_3"));
        }

        {
            Map<String, String> override = Maps.newHashMap();
            override.put("ph_4", "${prop4}/${prop_d}:${prop3}/${prop_c}");
            KylinConfigExt kylinConfigExt = KylinConfigExt.createInstance(kylinConfig, override);

            Assert.assertEquals("${prop4}/${prop_d}:prop_3/${prop_C}", kylinConfigExt.getOptional("ph_4"));

            kylinConfigExt.getExtendedOverrides().put("prop_d", "prop_D");
            kylinConfigExt.getExtendedOverrides().put("prop4", "${prop_d}");
            Assert.assertEquals("prop_D/prop_D:prop_3/${prop_C}", kylinConfigExt.getOptional("ph_4"));
        }

        {
            kylinConfig.setProperty("ph_5", "${prop5}");
            Assert.assertEquals("${prop5}", kylinConfig.getOptional("ph_5"));

            kylinConfig.setProperty("prop_d", "${ph_5}");
            kylinConfig.setProperty("prop5", "${prop_d}");
            Assertions.assertThrows(IllegalStateException.class, () -> {
                kylinConfig.getOptional("ph_5");
            });
        }
    }

    void updateProperty(String key, String value) {
        File propFile = KylinConfig.getSitePropertiesFile();
        Properties conf = new Properties();

        //load
        try (FileInputStream is = new FileInputStream(propFile)) {
            conf.load(is);
            conf.setProperty(key, value);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        //store
        try (FileOutputStream out = new FileOutputStream(propFile)) {
            conf.store(out, null);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
