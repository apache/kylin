/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.query

import org.apache.commons.io.FileUtils

import java.io.{File, IOException}

object QueryFetcher {
  @throws[IOException]
  def fetchQueries(folder: String): Array[(String, String)] = {
    var sqlFolder = new File(folder)
    if (!sqlFolder.exists()) {
      sqlFolder = new File(s"../$folder")
    }
    retrieveITSqls(sqlFolder)
  }


  @throws[IOException]
  private def retrieveITSqls(file: File): Array[(String, String)] = {
    if (file.exists() && file.listFiles() != null) {
      val sqlFiles = file.listFiles().filter(_.getName.endsWith(".sql"))
      sqlFiles.sortBy(_.getName).map(file => (file.getCanonicalPath, FileUtils.readFileToString(file, "UTF-8")))
    } else {
      throw new IllegalArgumentException("Error sql dir :" + file.getCanonicalPath)
    }
  }
}
