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
package org.apache.kylin.engine.spark.utils


import com.google.common.io.Files
import org.apache.kylin.engine.spark.cleanup.SnapshotChecker
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.sql.common.SparderBaseFunSuite
import org.junit.Assert

class HDFSUtilsTest extends SparderBaseFunSuite {
  private val fs: FileSystem = FileSystem.get(HadoopUtil.getCurrentConfiguration)
  val parentPath = new Path(Files.createTempDir().getCanonicalPath, "testDfs")

  override def beforeAll(): Unit = {
    val currentTime = System.currentTimeMillis() - DateUtils.MILLIS_PER_DAY * 9
    Range(0, 10).foreach { beforeDay =>
      Thread.sleep(1000L)
      createEmptyFileWithSpecialTS(fs,
        new Path(parentPath, beforeDay.toString), currentTime + DateUtils.MILLIS_PER_DAY * beforeDay)
    }
  }

  override def afterAll(): Unit = {
    fs.delete(parentPath, true)
  }

  test("list all files") {
    val statuses = HDFSUtils.listSortedFileFrom(parentPath)
    Assert.assertTrue(statuses.map(_.getPath.getName.toInt) == Range(0, 10).toList)
  }


  test("find last file") {
    val lastFile = HDFSUtils.findLastFile(parentPath)
    Assert.assertTrue(lastFile.getPath.getName.toInt == 9)
  }

  test("clean up snapshot table") {
    val lastFile = HDFSUtils.findLastFile(parentPath)
    val time = lastFile.getModificationTime
    HDFSUtils.deleteFilesWithCheck(parentPath, SnapshotChecker(3, 259200000L, time))
    val statuses = HDFSUtils.listSortedFileFrom(parentPath)
    // 6 7 8 9
    Assert.assertTrue(statuses.size == 4)
    Assert.assertTrue(statuses.map(_.getPath.getName.toInt) == Range(6, 10).toList)
  }

  def createEmptyFileWithSpecialTS(fileSystem: FileSystem, path: Path, timestamp: Long): Unit = {
    createEmptyFile(fileSystem, path)
    fileSystem.setTimes(path, timestamp, -1)
  }


  def createEmptyFile(fileSystem: FileSystem, path: Path): Unit = {
    val stream = fileSystem.create(path)
    stream.write(1)
    stream.close()
  }
}
