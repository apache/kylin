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

package org.apache.kylin.engine.spark.job.stage.build.partition

import io.kyligence.kap.guava20.shaded.common.collect.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.engine.spark.builder.{DictionaryBuilderHelper, PartitionDictionaryBuilderHelper}
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import org.apache.kylin.engine.spark.job.{PartitionExec, SegmentJob}
import org.apache.kylin.engine.spark.model.PartitionFlatTableDesc
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree
import org.apache.kylin.metadata.cube.cuboid.PartitionSpanningTree.PartitionTreeBuilder
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.{Dataset, Row}

import java.util.Objects
import java.{lang, util}
import scala.collection.JavaConverters._

abstract class PartitionFlatTableAndDictBase(private val jobContext: SegmentJob,
                                             private val dataSegment: NDataSegment,
                                             private val buildParam: BuildParam)
  extends FlatTableAndDictBase(jobContext, dataSegment, buildParam) with PartitionExec {

  protected final val newBuckets = //
    jobContext.getReadOnlyBuckets.asScala.filter(_.getSegmentId.equals(segmentId)).toSeq


  override protected lazy val spanningTree = buildParam.getPartitionSpanningTree
  override protected lazy val tableDesc = buildParam.getTableDesc

  override protected def applyPartitionDesc(originDS: Dataset[Row]): Dataset[Row] = {
    // Multi level partition.
    val descMLP = dataModel.getMultiPartitionDesc
    require(Objects.nonNull(descMLP))
    // Date range partition.
    val descDRP = dataModel.getPartitionDesc
    val condition = descMLP.getPartitionConditionBuilder
      .buildMultiPartitionCondition(descDRP, descMLP, //
        new util.LinkedList[lang.Long](tableDesc.asInstanceOf[PartitionFlatTableDesc].getPartitions), null, segmentRange)
    if (StringUtils.isBlank(condition)) {
      logInfo(s"Segment $segmentId no available partition condition.")
      return originDS
    }
    logInfo(s"Segment $segmentId apply partition condition $condition.")
    originDS.where(condition)
  }

  def gatherPartitionStatistics(partition: Long, tableDS: Dataset[Row]): Statistics = {
    val desc = s"Segment $segmentId collect partition flat table statistics $partition"
    logInfo(desc)
    sparkSession.sparkContext.setJobDescription(desc)
    val statistics = gatherStatistics(tableDS)
    sparkSession.sparkContext.setJobDescription(null)
    logInfo(s"$desc $statistics")
    statistics
  }

  def getPartitionDS(partition: Long): Dataset[Row] = {
    val columnIds = tableDesc.getColumnIds.asScala
    val columnName2Id = tableDesc.getColumns //
      .asScala //
      .map(column => column.getIdentity) //
      .zip(columnIds) //
    val column2IdMap = columnName2Id.toMap

    val partitionColumnIds = dataModel.getMultiPartitionDesc.getColumnRefs.asScala //
      .map(_.getIdentity).map(x => column2IdMap.apply(x))
    val values = dataModel.getMultiPartitionDesc.getPartitionInfo(partition).getValues.toSeq

    val converted = partitionColumnIds.zip(values).map { case (k, v) =>
      s"`$k` = '$v'"
    }.mkString(" and ")

    logInfo(s"Segment $segmentId single partition condition: $converted")
    FLAT_TABLE.where(converted)
  }

  override def prepareForDict(): (Set[TblColRef], Set[TblColRef], Set[TblColRef], Set[TblColRef]) = {
    val dictCols = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(dataSegment, spanningTree.getIndices).asScala.toSet
    val encodeCols = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(dataSegment, spanningTree.getIndices).asScala.toSet
    val dictColsWithoutCc = dictCols.filter(!_.getColumnDesc.isComputedColumn)
    val encodeColsWithoutCc = encodeCols.filter(!_.getColumnDesc.isComputedColumn)
    (dictCols, encodeCols, dictColsWithoutCc, encodeColsWithoutCc)
  }

  override def initSpanningTree(): Unit = {
    val spanTree = new PartitionSpanningTree(config, //
      new PartitionTreeBuilder(dataSegment, readOnlyLayouts, jobId, partitions, Sets.newHashSet(newBuckets.asJava)))
    buildParam.setPartitionSpanningTree(spanTree)
  }

  override def initFlatTableDesc(): Unit = {
    val tableDesc = if (jobContext.isPartialBuild) {
      val parser = new IndexDependencyParser(dataModel)
      val relatedTableAlias =
        parser.getRelatedTablesAlias(jobContext.getReadOnlyLayouts)
      new PartitionFlatTableDesc(config, dataSegment, spanningTree, relatedTableAlias, jobId, partitions)
    } else {
      new PartitionFlatTableDesc(config, dataSegment, spanningTree, jobId, partitions)
    }
    buildParam.setTableDesc(tableDesc)
  }
}
