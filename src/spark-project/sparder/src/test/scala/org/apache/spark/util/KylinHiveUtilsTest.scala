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

package org.apache.spark.util

import org.apache.kylin.guava30.shaded.common.io.Files
import org.apache.commons.io.FileUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.Unsafe.overwriteSystemProp
import org.apache.kylin.common.util.{TempMetadataBuilder, Unsafe}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}

import java.io.File

class KylinHiveUtilsTest extends SparderBaseFunSuite with SharedSparkSession{

  test("testHive") {
    val tmpHome = Files.createTempDir
    FileUtils.touch(new File(tmpHome.getAbsolutePath + "/kylin.properties"))
    KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath)
    val kylinConfig = KylinConfig.getInstanceFromEnv
    kylinConfig.setProperty("kylin.env", "UT")
    KylinHiveUtils.checkHiveIsAccessible("show databases");
  }

}
