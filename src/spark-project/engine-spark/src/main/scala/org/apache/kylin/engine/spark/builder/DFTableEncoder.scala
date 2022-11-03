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

import org.apache.kylin.engine.spark.builder.DFBuilderHelper.ENCODE_SUFFIX
import org.apache.kylin.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.dict.NGlobalDictionaryV2
import org.apache.spark.internal.Logging
import org.apache.spark.sql.KapFunctions._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.sql.{Dataset, Row}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable._

object DFTableEncoder extends Logging {

  def encodeTable(ds: Dataset[Row], seg: NDataSegment, cols: util.Set[TblColRef]): Dataset[Row] = {
    val structType = ds.schema
    var partitionedDs = ds

    ds.sparkSession.sparkContext.setJobDescription("Encode count source data.")
    val sourceCnt = ds.count()
    val bucketThreshold = seg.getConfig.getGlobalDictV2ThresholdBucketSize
    val minBucketSize: Long = sourceCnt / bucketThreshold

    var encodingCols = scala.collection.mutable.Set.empty[TblColRef]

    if (seg.getIndexPlan.isSkipEncodeIntegerFamilyEnabled) {
      encodingCols = cols.asScala.filterNot(_.getType.isIntegerFamily)

      val noEncodeCols = cols.asScala.filter(_.getType.isIntegerFamily).map {
        ref =>
          val encodeColRef = convertFromDot(ref.getBackTickIdentity)
          val aliasName = encodeColRef.concat(ENCODE_SUFFIX)
          col(encodeColRef).cast(LongType).as(aliasName)
      }.toSeq
      partitionedDs = partitionedDs.select(partitionedDs.schema.map(ty => col(ty.name)) ++ noEncodeCols: _*)
    } else {
      encodingCols = cols.asScala
    }

    val encodingArgs = encodingCols.map {
      ref =>
        val globalDict = new NGlobalDictionaryV2(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
        val bucketSize = globalDict.getBucketSizeOrDefault(seg.getConfig.getGlobalDictV2MinHashPartitions)
        val enlargedBucketSize = (((minBucketSize / bucketSize) + 1) * bucketSize).toInt

        val encodeColRef = convertFromDot(ref.getBackTickIdentity)
        val columnIndex = structType.fieldIndex(encodeColRef)

        val dictParams = Array(seg.getProject, ref.getTable, ref.getName, seg.getConfig.getHdfsWorkingDirectory)
          .mkString(SEPARATOR)
        val aliasName = structType.apply(columnIndex).name.concat(ENCODE_SUFFIX)
        val encodeCol = dict_encode(col(encodeColRef).cast(StringType), lit(dictParams), lit(bucketSize).cast(StringType)).as(aliasName)
        val columns = encodeCol
        (enlargedBucketSize, col(encodeColRef).cast(StringType), columns, aliasName,
          bucketSize == 1)
    }

    encodingArgs.foreach {
      case (enlargedBucketSize, repartitionColumn, projects, _, false) =>
        partitionedDs = partitionedDs
          .repartition(enlargedBucketSize, repartitionColumn)
          .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
      case (_, _, projects, _, true) =>
        partitionedDs = partitionedDs
          .select(partitionedDs.schema.map(ty => col(ty.name)) ++ Seq(projects): _*)
    }
    ds.sparkSession.sparkContext.setJobDescription(null)
    partitionedDs
  }
}
