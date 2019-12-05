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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.common.SharedSparkSession
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation}
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

class TestResourceDetectUtilsByMock extends WordSpec with MockFactory with SharedSparkSession {
  "getPaths" when {
    "FileSourceScanExec" should {
      "get right paths" in {
        val paths = Seq(new Path("test"))
        val fileIndex = mock[FileIndex]
        (fileIndex.rootPaths _).expects().returning(paths).anyNumberOfTimes()
        val relation = HadoopFsRelation(fileIndex, new StructType(), new StructType(), null, null, null)(spark)
        val sparkPlan = FileSourceScanExec(relation, null, null, null, null, Seq.empty, Option(new TableIdentifier("table"))
        )
        assert(paths == ResourceDetectUtils.getPaths(sparkPlan))
      }
    }
  }
}
