/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.spark.sql.hive.utils

import java.io.FileOutputStream
import java.util.{List => JList, Map => JMap}

import com.google.common.collect.{Lists, Maps}
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.apache.spark.util.Utils

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
        out.write(contents.apply(i).getBytes)
        out.close()
      }
      val l = ResourceDetectUtils.getResourceSize(files.head, files.last)
      assert(l == contents.map(_.getBytes.length).sum)
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
        out.write(contents.apply(i).getBytes)
        out.close()
      }
      import scala.collection.JavaConverters._

     val l = resourcePaths.values().asScala.map(path => ResourceDetectUtils.getResourceSize(new Path(path.get(0)))).max
      assert(l == contents.last.getBytes.length)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
