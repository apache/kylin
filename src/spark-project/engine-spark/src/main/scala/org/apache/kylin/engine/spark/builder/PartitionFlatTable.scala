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

import org.apache.kylin.engine.spark.model.PartitionFlatTableDesc
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.util.Objects
import java.{lang, util}
import scala.collection.JavaConverters._

class PartitionFlatTable(private val sparkSession: SparkSession, //
                         private val tableDesc: PartitionFlatTableDesc) extends SegmentFlatTable(sparkSession, tableDesc) {

  override protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Multi level partition.
    val descMLP = dataModel.getMultiPartitionDesc
    require(Objects.nonNull(descMLP))
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    val condition = descMLP.getPartitionConditionBuilder
      .buildMultiPartitionCondition(descDRP, descMLP, //
        new util.LinkedList[lang.Long](tableDesc.getPartitions), null, segmentRange)
    if (StringUtils.isBlank(condition)) {
      logInfo(s"Segment $segmentId no available partition condition.")
      return originDS
    }
    logInfo(s"Segment $segmentId apply partition condition $condition.")
    originDS.where(condition)
  }

  def getPartitionDS(partitionId: Long): Dataset[Row] = {
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns //
      .asScala //
      .map(column => column.getIdentity) //
      .zip(columnIds) //
    val column2IdMap = columnName2Id.toMap

    val partitionColumnIds = dataModel.getMultiPartitionDesc.getColumnRefs.asScala //
      .map(_.getIdentity).map(x => column2IdMap.apply(x))
    val values = dataModel.getMultiPartitionDesc.getPartitionInfo(partitionId).getValues.toSeq

    val converted = partitionColumnIds.zip(values).map { case (k, v) =>
      s"`$k` = '$v'"
    }.mkString(" and ")

    logInfo(s"Segment $segmentId single partition condition: $converted")
    FLAT_TABLE.where(converted)
  }

}
