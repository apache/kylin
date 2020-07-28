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
package org.apache.kylin.engine.spark.builder

import java.util

import org.apache.kylin.engine.spark.builder.CubeBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.engine.spark.metadata.{ColumnDesc, SegmentInfo}
import org.apache.spark.dict.NGlobalDictionary
import org.apache.spark.internal.Logging
import org.apache.spark.sql.KylinFunctions._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable._

object CubeTableEncoder extends Logging {

  def encodeTable(ds: Dataset[Row], seg: SegmentInfo, cols: util.Set[ColumnDesc]): Dataset[Row] = {
    val structType = ds.schema
    var partitionedDs = ds

    ds.sparkSession.sparkContext.setJobDescription("Encode count source data.")
    val sourceCnt = ds.count()
    val bucketThreshold = seg.kylinconf.getGlobalDictV2ThresholdBucketSize
    val minBucketSize: Long = sourceCnt / bucketThreshold

    cols.asScala.foreach(
      ref => {
        val globalDict = new NGlobalDictionary(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory)
        val bucketSize = globalDict.getBucketSizeOrDefault(seg.kylinconf.getGlobalDictV2MinHashPartitions)
        val enlargedBucketSize = (((minBucketSize / bucketSize) + 1) * bucketSize).toInt

        val encodeColRef = convertFromDot(ref.identity)
        val columnIndex = structType.fieldIndex(encodeColRef)

        val dictParams = Array(seg.project, ref.tableAliasName, ref.columnName, seg.kylinconf.getHdfsWorkingDirectory)
          .mkString(SEPARATOR)
        val aliasName = structType.apply(columnIndex).name.concat(ENCODE_SUFFIX)
        val encodeCol = dict_encode(col(encodeColRef).cast(StringType), lit(dictParams), lit(bucketSize).cast(StringType)).as(aliasName)
        val columns = partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(encodeCol)

        partitionedDs = partitionedDs
          .repartition(enlargedBucketSize, col(encodeColRef).cast(StringType))
          .select(columns: _*)
      }
    )
    ds.sparkSession.sparkContext.setJobDescription(null)
    partitionedDs
  }
}