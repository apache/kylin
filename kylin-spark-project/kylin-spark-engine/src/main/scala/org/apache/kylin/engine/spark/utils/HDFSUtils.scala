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

import java.io.FileNotFoundException

import org.apache.kylin.engine.spark.cleanup.{HDFSResourceCheck, SnapshotChecker}
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
