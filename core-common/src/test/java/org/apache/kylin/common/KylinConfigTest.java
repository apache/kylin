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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.junit.Assert;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class KylinConfigTest extends HotLoadKylinPropertiesTestCase {

    @Test
    public void testMRConfigOverride() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = config.getMRConfigOverride();
        assertEquals(2, override.size());
        assertEquals("test1", override.get("test1"));
        assertEquals("test2", override.get("test2"));
    }

    @Test
    public void testBackwardCompatibility() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String oldk = "kylin.test.bcc.old.key";
        final String newk = "kylin.test.bcc.new.key";

        assertNull(config.getOptional(oldk));
        assertNotNull(config.getOptional(newk));

        Map<String, String> override = Maps.newHashMap();
        override.put(oldk, "1");
        KylinConfigExt ext = KylinConfigExt.createInstance(config, override);
        assertEquals(ext.getOptional(oldk), null);
        assertEquals(ext.getOptional(newk), "1");
        assertNotEquals(config.getOptional(newk), "1");

        config.setProperty(oldk, "2");
        assertEquals(config.getOptional(newk), "2");
    }

    @Test
    public void testExtShareTheBase() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = Maps.newHashMap();
        KylinConfig configExt = KylinConfigExt.createInstance(config, override);
        assertTrue(config.properties == configExt.properties);
        config.setProperty("1234", "1234");
        assertEquals("1234", configExt.getOptional("1234"));
    }

    @Test
    public void testPropertiesHotLoad() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        assertEquals("whoami@kylin.apache.org", config.getKylinOwner());

        updateProperty("kylin.storage.hbase.owner-tag", "kylin@kylin.apache.org");
        KylinConfig.getInstanceFromEnv().reloadFromSiteProperties();

        assertEquals("kylin@kylin.apache.org", config.getKylinOwner());
    }

    @Test
    public void testGetMetadataUrlPrefix() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        config.setMetadataUrl("testMetaPrefix@hbase");
        assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("testMetaPrefix@hdfs");
        assertEquals("testMetaPrefix", config.getMetadataUrlPrefix());

        config.setMetadataUrl("/kylin/temp");
        assertEquals("/kylin/temp", config.getMetadataUrlPrefix());
    }

    @Test
    public void testThreadLocalOverride() throws InterruptedException {
        final String metadata1 = "meta1";
        final String metadata2 = "meta2";

        // set system KylinConfig
        KylinConfig sysConfig = KylinConfig.getInstanceFromEnv();
        sysConfig.setMetadataUrl(metadata1);

        assertEquals(metadata1, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());

        // test thread-local override
        KylinConfig threadConfig = KylinConfig.createKylinConfig(new Properties());
        threadConfig.setMetadataUrl(metadata2);
        
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(threadConfig)) {

            assertEquals(metadata2, KylinConfig.getInstanceFromEnv().getMetadataUrl().toString());
    
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
            assertEquals(metadata1, holder[0]);
        }
    }

    @Test
    public void testHdfsWorkingDir() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        String hdfsWorkingDirectory = conf.getHdfsWorkingDirectory();
        assertTrue(hdfsWorkingDirectory.startsWith("file:/"));
    }

    @Test
    public void testUnexpectedBlackInPro() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        Map<String, String> override = conf.getPropertiesByPrefix("kylin.engine.mr.config-override.");
        assertEquals(2, override.size());
        String s = override.get("test2");
        assertEquals("test2", s);
    }

    @Test
    public void testCalciteExtrasProperties() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        Properties extras = conf.getCalciteExtrasProperties();
        assertEquals("true", extras.getProperty("caseSensitive"));
        assertEquals("TO_UPPER", extras.getProperty("unquotedCasing"));
        assertEquals("DOUBLE_QUOTE", extras.getProperty("quoting"));
        assertEquals("LENIENT", extras.getProperty("conformance"));

        conf.setProperty("kylin.query.calcite.extras-props.caseSensitive", "false");
        conf.setProperty("kylin.query.calcite.extras-props.unquotedCasing", "UNCHANGED");
        conf.setProperty("kylin.query.calcite.extras-props.quoting", "BRACKET");
        conf.setProperty("kylin.query.calcite.extras-props.conformance", "DEFAULT");
        extras = conf.getCalciteExtrasProperties();
        assertEquals("false", extras.getProperty("caseSensitive"));
        assertEquals("UNCHANGED", extras.getProperty("unquotedCasing"));
        assertEquals("BRACKET", extras.getProperty("quoting"));
        assertEquals("DEFAULT", extras.getProperty("conformance"));
    }

  @Test
  public void testSetKylinConfigInEnvIfMissingTakingEmptyProperties() {
      Properties properties = new Properties();
      KylinConfig.setKylinConfigInEnvIfMissing(properties);

      assertEquals(0, properties.size());
      assertTrue(properties.isEmpty());

      KylinConfig.setKylinConfigInEnvIfMissing(properties);

      assertEquals(0, properties.size());
      assertTrue(properties.isEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testCreateInstanceFromUriThrowsIllegalStateExceptionOne() {
        KylinConfig.createInstanceFromUri("cb%MnlG]3:nav");
  }

  @Test(expected = RuntimeException.class)
  public void testCreateInstanceFromUriThrowsRuntimeException() {
      Properties properties = new Properties();
      KylinConfig.setKylinConfigInEnvIfMissing(properties);

      assertEquals(0, properties.size());
      assertTrue(properties.isEmpty());

      KylinConfig.createInstanceFromUri("dHy3K~m");
  }

  @Test
  public void testKylinConfigExt(){
      KylinConfig conf = KylinConfig.getInstanceFromEnv();
      Map<String, String> overrideConf1 = new HashMap<>();
      overrideConf1.put("foo", "fooValue");
      overrideConf1.put("bar", "");
      KylinConfigExt ext1 = KylinConfigExt.createInstance(conf, overrideConf1);

      Map<String, String> overrideConf2 = new HashMap<>();
      overrideConf2.put("bar", "barValue");
      KylinConfigExt ext2 = KylinConfigExt.createInstance(ext1, overrideConf2);

      //check previous ext config's override value will not lost
      Assert.assertEquals(ext2.getOptional("foo"), "fooValue");
      //check previous exist config will be overwritten by by new one
      Assert.assertEquals(ext2.getOptional("bar"), "barValue");
  }

}
