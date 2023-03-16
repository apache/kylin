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

package org.apache.spark.sql.hive.utils

import java.io.FileOutputStream
import java.util.{List => JList, Map => JMap}
import org.apache.kylin.guava30.shaded.common.collect.{Lists, Maps}
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.NLocalFileMetadataTestCase
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.util.Utils

import java.nio.charset.Charset

class TestResourceDetectUtils extends SparderBaseFunSuite {
  private var config: KylinConfig = _

  override def beforeAll(): Unit = {
    NLocalFileMetadataTestCase.staticCreateTestMetadata()
    config = KylinConfig.getInstanceFromEnv
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    NLocalFileMetadataTestCase.staticCleanupTestMetadata()
    super.afterAll()
  }


  test("write and read resource paths") {
    val map: JMap[String, JList[String]] = Maps.newHashMap()
    map.put("test", Lists.newArrayList("test"))
    withTempPath { file =>
      val path = new Path(file.getPath)
      ResourceDetectUtils.write(path, map)
      val actualMap: JMap[String, JList[String]] = ResourceDetectUtils.readResourcePathsAs(path)
      assert(map == actualMap)
    }
  }

  test("write and read large size (more than 65535) resource paths") {
    val map: JMap[String, JList[String]] = Maps.newHashMap()
    val bytes = Array.fill(100000)('1')
    map.put("test", Lists.newArrayList(bytes.mkString))
    withTempPath { file =>
      val path = new Path(file.getPath)
      ResourceDetectUtils.write(path, map)
      val actualMap: JMap[String, JList[String]] = ResourceDetectUtils.readResourcePathsAs(path)
      assert(map == actualMap)
    }
  }

  test("getResourceSize") {
    val contents = List("test", "test_test_test")
    val tempDir = Utils.createTempDir()
    val files = List(new Path(tempDir.getPath, "test1"), new Path(tempDir.getPath, "test2"))
    try {
      for (i <- 0 to 1) {
        val out = new FileOutputStream(files.apply(i).toString)
        out.write(contents.apply(i).getBytes(Charset.defaultCharset()))
        out.close()
      }
      var l = ResourceDetectUtils.getResourceSize(false, files.head, files.last)
      assert(l == contents.map(_.getBytes(Charset.defaultCharset()).length).sum)
      l = ResourceDetectUtils.getResourceSize(true, files.head, files.last)
      assert(l == contents.map(_.getBytes(Charset.defaultCharset()).length).sum)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  test("getMaxResourceSize") {
    val contents = List("test", "test_test_test")
    val tempDir = Utils.createTempDir()
    val path1 = new Path(tempDir.getPath, "test1")
    val path2 = new Path(tempDir.getPath, "test2")
    val files = List(path1, path2)
    val resourcePaths: JMap[String, JList[String]] = Maps.newHashMap()
    resourcePaths.put("1", Lists.newArrayList(path1.toString))
    resourcePaths.put("2", Lists.newArrayList(path2.toString))
    try {
      for (i <- 0 to 1) {
        val out = new FileOutputStream(files.apply(i).toString)
        out.write(contents.apply(i).getBytes(Charset.defaultCharset()))
        out.close()
      }
      import scala.collection.JavaConverters._

      val l = resourcePaths.values().asScala.map(path => ResourceDetectUtils.getResourceSize(false,
        new Path(path.get(0)))).max
      assert(l == contents.last.getBytes(Charset.defaultCharset()).length)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
