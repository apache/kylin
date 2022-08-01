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
package org.apache.spark.sql.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class ResourceDetectUtilsSuite extends SparderBaseFunSuite with SharedSparkSession {

  test("getPathsTest") {
    val session = spark.newSession()
    val tempPath = Utils.createTempDir().getAbsolutePath
    val pathSeq = Seq(new Path(tempPath))
    val relation = HadoopFsRelation(
      location = new InMemoryFileIndex(spark, pathSeq, Map.empty, None),
      partitionSchema = PartitionSpec.emptySpec.partitionColumns,
      dataSchema = StructType.fromAttributes(Nil),
      bucketSpec = Some(BucketSpec(2, Nil, Nil)),
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
    val layoutFileSourceScanExec =
      LayoutFileSourceScanExec(relation, Nil,
        relation.dataSchema, Nil, None, None, Nil, None)
    val paths = ResourceDetectUtils.getPaths(layoutFileSourceScanExec)
    assert(1 == paths.size)
    assert(tempPath == paths.apply(0).toString)
  }

}
