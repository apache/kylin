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

package org.apache.kylin.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;


import static org.junit.Assert.*;

/**
 * Created by sunyerui on 15/8/26.
 * Tests for HadoopUtil
 */
public class HadoopUtilTest {

  @BeforeClass
  public static void beforeClass() {
    System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/sandbox");
  }

  @After
  public void after() {
    HadoopUtil.setCurrentConfiguration(null);
    HadoopUtil.setCurrentHBaseConfiguration(null);
  }

  @Test
  public void testGetCurrentHBaseConfiguration() throws Exception {
    KylinConfig config = KylinConfig.getInstanceFromEnv();
    config.setProperty(KylinConfig.KYLIN_HBASE_CLUSTER_FS, "hdfs://hbase-cluster/");

    Configuration conf = HadoopUtil.getCurrentHBaseConfiguration();
    assertEquals("hdfs://hbase-cluster/", conf.get(FileSystem.FS_DEFAULT_NAME_KEY));
  }

  @Test
  public void testMakeQualifiedPathInHBaseCluster() throws Exception {
    KylinConfig config = KylinConfig.getInstanceFromEnv();
    config.setProperty(KylinConfig.KYLIN_HBASE_CLUSTER_FS, "file:/");

    String path = HadoopUtil.makeQualifiedPathInHBaseCluster("/path/to/test/hbase");
    assertEquals("file:/path/to/test/hbase", path);
  }
}
