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
package io.kyligence.kap.engine.spark.utils


import com.google.common.io.Files
import io.kyligence.kap.engine.spark.cleanup.SnapshotChecker
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
