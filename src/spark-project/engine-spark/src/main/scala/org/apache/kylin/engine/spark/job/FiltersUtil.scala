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

package org.apache.kylin.engine.spark.job

import org.apache.kylin.engine.spark.builder.CreateFlatTable
import org.apache.kylin.engine.spark.model.SegmentFlatTableDesc
import org.apache.kylin.metadata.model.{JoinTableDesc, PartitionDesc, TblColRef}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.Set

object FiltersUtil extends Logging {

  private val allEqualsColSets: Set[String] = Set.empty


  private var flatTable: SegmentFlatTableDesc = _

  def initFilters(flatTable: SegmentFlatTableDesc,
                  lookupTableDatasetMap: mutable.Map[JoinTableDesc, Dataset[Row]]): Unit = {
    try {
      this.flatTable = flatTable
      if (flatTable.getDataModel.getPartitionDesc == null) {
        return
      }
      val parColumn = flatTable.getDataModel.getPartitionDesc.getPartitionDateColumnRef.toString
      logInfo(s"partition col is ${parColumn}")
      val allEqualColPairs = lookupTableDatasetMap.map(_._1.getJoin).
        flatMap(join => join.getForeignKeyColumns.map(_.toString).zip(join.getPrimaryKeyColumns.map(_.toString)))
      logInfo(s"allEqualColPairs is ${allEqualColPairs} ")
      allEqualsColSets += parColumn
      var singleRoundEqualColSets = allEqualColPairs.filter(_._1 == parColumn).map(_._2).toSet

      logInfo(s"first round equal col sets is ${singleRoundEqualColSets}")

      var subSet = singleRoundEqualColSets -- allEqualsColSets

      while (subSet.nonEmpty) {
        subSet = singleRoundEqualColSets -- allEqualsColSets
        logInfo(s"this round substract equal col set is ${subSet}")
        allEqualsColSets ++= subSet
        singleRoundEqualColSets = subSet.flatMap { col =>
          allEqualColPairs.filter(_._1 == col).map(_._2)
        }
        logInfo(s"this round equal col sets is ${singleRoundEqualColSets}")
      }
      logInfo(s"the allEqualsColSets is ${allEqualsColSets}")
    } catch {
      case e: Exception =>
        log.warn("init filters failed: Exception: ", e)
    }

  }

  def getAllEqualColSets(): Set[String] = {
    allEqualsColSets
  }

  def inferFilters(pks: Array[TblColRef],
                   ds: Dataset[Row]): Dataset[Row] = {
    // just consider one join key condition
    pks.filter(pk => allEqualsColSets.contains(pk.toString)).headOption match {
      case Some(col) =>
        var afterFilter = ds
        val model = flatTable.getDataModel
        val partDesc = PartitionDesc.getCopyOf(model.getPartitionDesc)
        partDesc.setPartitionDateColumnRef(col)
        if (partDesc != null && partDesc.getPartitionDateColumn != null) {
          val segRange = flatTable.getSegmentRange
          if (segRange != null && !segRange.isInfinite) {
            var afterConvertPartition = partDesc.getPartitionConditionBuilder
              .buildDateRangeCondition(partDesc, null, segRange)
            afterConvertPartition = CreateFlatTable.replaceDot(afterConvertPartition, model)
            logInfo(s"Partition filter $afterConvertPartition")
            afterFilter = afterFilter.where(afterConvertPartition)
          }
        }
        logInfo(s"after filter plan is ${
          afterFilter.queryExecution
        }")
        afterFilter
      case None =>
        ds
    }
  }
}
