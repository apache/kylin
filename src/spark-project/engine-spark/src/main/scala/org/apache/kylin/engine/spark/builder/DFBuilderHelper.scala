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

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.metadata.cube.model.{NDataSegment, NDataflowManager, NDataflowUpdate}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, Dataset, Row}

import scala.util.{Failure, Success, Try}

object DFBuilderHelper extends Logging {

  val ENCODE_SUFFIX = "_KE_ENCODE"

  /**
   * select columns to build
   * 1. exclude columns on the fact table
   * 2. exclude columns (without CC) on the lookup tables
   */
  def selectColumnsNotInTables(factTable: Dataset[Row], lookupTables: Seq[Dataset[Row]], cols: Set[TblColRef]): Set[TblColRef] = {

    var remainedCols = cols
    remainedCols = remainedCols -- selectColumnsInTable(factTable, cols)

    val colsWithoutCc = cols.filter(!_.getColumnDesc.isComputedColumn)
    remainedCols = remainedCols -- lookupTables.flatMap(ds => selectColumnsInTable(ds, colsWithoutCc))

    remainedCols
  }

  def selectColumnsInTable(table: Dataset[Row], columns: Set[TblColRef]): Set[TblColRef] = {
    columns.filter(col =>
      isColumnInTable(NSparkCubingUtil.convertFromDotWithBackTick(col.getBackTickExp), table))
  }

  // ============================= Used by {@link DFBuildJob}.Functions are deprecated. ========================= //
  @deprecated
  def filterCols(dsSeq: Seq[Dataset[Row]], needCheckCols: Set[TblColRef]): Set[TblColRef] = {
    needCheckCols -- dsSeq.flatMap(ds => selectColumnsInTable(ds, needCheckCols))
  }

  @deprecated
  def filterOutIntegerFamilyType(table: Dataset[Row], columns: Set[TblColRef]): Set[TblColRef] = {
    columns.filterNot(_.getType.isIntegerFamily).filter(cc =>
      isColumnInTable(NSparkCubingUtil.convertFromDot(cc.getBackTickExp), table))
  }

  private def isColumnInTable(colExpr: String, table: Dataset[Row]): Boolean = {
    Try(table.select(expr(colExpr))) match {
      case Success(_) =>
        true
      case Failure(_) =>
        false
    }
  }

  def chooseSuitableCols(ds: Dataset[Row], needCheckCols: Iterable[TblColRef]): Seq[Column] = {
    needCheckCols
      .filter(ref => isColumnInTable(ref.getBackTickExp, ds))
      .map(ref => expr(NSparkCubingUtil.convertFromDotWithBackTick(ref.getBackTickExp))
        .alias(NSparkCubingUtil.convertFromDot(ref.getBackTickIdentity)))
      .toSeq
  }

  def checkPointSegment(readOnlySeg: NDataSegment, checkpointOps: NDataSegment => Unit): NDataSegment = {
    // read basic infos from the origin segment
    val segId = readOnlySeg.getId
    val dfId = readOnlySeg.getDataflow.getId
    val project = readOnlySeg.getProject

    // read the current config
    // this config is initialized at SparkApplication in which the HDFSMetaStore has been specified
    val config = KylinConfig.getInstanceFromEnv

    // copy the latest df & seg
    val dfCopy = NDataflowManager.getInstance(config, project).getDataflow(dfId).copy()
    val segCopy = dfCopy.getSegment(segId)
    val dfUpdate = new NDataflowUpdate(dfId)
    checkpointOps(segCopy)
    dfUpdate.setToUpdateSegs(segCopy)

    // define the updating operations
    class DataFlowUpdateOps extends UnitOfWork.Callback[NDataSegment] {
      override def process(): NDataSegment = {
        val updatedDf = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, project).updateDataflow(dfUpdate)
        updatedDf.getSegment(segId)
      }
    }

    // temporarily for ut
    // return the latest segment
    UnitOfWork.doInTransactionWithRetry(new DataFlowUpdateOps, project)
  }
}
