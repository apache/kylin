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

import java.io.FileNotFoundException

import io.kyligence.kap.engine.spark.cleanup.{HDFSResourceCheck, SnapshotChecker}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, Path}
import org.apache.kylin.common.util.HadoopUtil
import org.apache.spark.internal.Logging

object HDFSUtils extends Logging {

  protected def getActiveHadoopConf: Configuration = HadoopUtil.getCurrentConfiguration

  protected def getFileContext(path: Path): FileContext = {
    FileContext.getFileContext(path.toUri, getActiveHadoopConf)
  }

  def listSortedFileFrom(path: Path): List[FileStatus] = {
    val fc = getFileContext(path)
    if (!fc.util.exists(path)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fc.util.listStatus(path)
    files.toList.sortBy(_.getModificationTime)
  }

  def findLastFile(path: Path): FileStatus = {
    listSortedFileFrom(path).last
  }


  def deleteFilesWithCheck(path: Path, HDFSResourceCheck: HDFSResourceCheck): Unit = {
    val context = getFileContext(path)
    HDFSResourceCheck.isExpired(listSortedFileFrom(path)).foreach { fs =>
      context.delete(fs.getPath, true)
      logInfo(s"Delete file ${fs.getPath}")
    }
  }

  def getFileStatus(path: Path): FileStatus = {
    getFileContext(path).getFileStatus(path)
  }
}
