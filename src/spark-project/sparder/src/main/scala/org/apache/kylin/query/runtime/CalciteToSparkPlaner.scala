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
package org.apache.kylin.query.runtime

import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.metadata.model.NDataModel.DataStorageType
import org.apache.kylin.query.relnode._
import org.apache.kylin.query.runtime.FilePruningMode.PruningMode
import org.apache.kylin.query.runtime.plan._
import org.apache.kylin.query.runtime.planV3.DeltaLakeTableScanPlan
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.util

class CalciteToSparkPlaner(dataContext: DataContext) extends RelVisitor with LogEx {
  private val stack = new util.ArrayDeque[LogicalPlan]()
  private val setOpStack = new util.ArrayDeque[Int]()
  private var unionLayer = 0
  private var storageType = DataStorageType.V1
  private lazy val filePruningMode = computeFilePruningMode

  // clear cache before any op
  cleanCache()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[OlapUnionRel]) {
      unionLayer = unionLayer + 1
    }
    if (node.isInstanceOf[OlapUnionRel] || node.isInstanceOf[OlapMinusRel]) {
      setOpStack.offerLast(stack.size())
    }
    // skip non runtime joins children
    // cases to skip children visit
    // 1. current node is a OlapJoinRel and is not a runtime join
    // 2. current node is a OlapNonEquiJoinRel and is not a runtime join
    if (!(node.isInstanceOf[OlapJoinRel] && !node.asInstanceOf[OlapJoinRel].isRuntimeJoin) &&
      !(node.isInstanceOf[OlapNonEquiJoinRel] && !node.asInstanceOf[OlapNonEquiJoinRel].isRuntimeJoin)) {
      node.childrenAccept(this)
    }
    stack.offerLast(node match {
      case rel: OlapTableScan => convertTableScan(rel)
      case rel: OlapFilterRel =>
        logTime("filter") {
          FilterPlan.filter(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapProjectRel =>
        logTime("project") {
          ProjectPlan.select(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapLimitRel =>
        logTime("limit") {
          LimitPlan.limit(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapSortRel =>
        logTime("sort") {
          SortPlan.sort(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapWindowRel =>
        logTime("window") {
          WindowPlan.window(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapAggregateRel =>
        logTime("agg") {
          AggregatePlan.agg(stack.pollLast(), rel)
        }
      case rel: OlapJoinRel => convertJoinRel(rel)
      case rel: OlapNonEquiJoinRel => convertNonEquiJoinRel(rel)
      case rel: OlapUnionRel =>
        val size = setOpStack.pollLast()
        var unionBlocks = Range(0, stack.size() - size).map(_ => stack.pollLast())
        if (KylinConfig.getInstanceFromEnv.isCollectUnionInOrder) {
          unionBlocks = unionBlocks.reverse
        }
        logTime("union") {
          plan.UnionPlan.union(unionBlocks, rel, dataContext)
        }
      case rel: OlapMinusRel =>
        val size = setOpStack.pollLast()
        logTime("minus") {
          plan.MinusPlan.minus(Range(0, stack.size() - size).map(_ => stack.pollLast()).reverse, rel, dataContext)
        }
      case rel: OlapValuesRel =>
        logTime("values") {
          ValuesPlan.values(rel)
        }
      case rel: OlapModelViewRel =>
        logTime("modelview") {
          stack.pollLast()
        }
    })
    if (node.isInstanceOf[OlapUnionRel]) {
      unionLayer = unionLayer - 1
    }
  }

  private def convertTableScan(rel: OlapTableScan): LogicalPlan = {
    val execFunc = rel.getContext.genExecFunc(rel)

    val tablePlan = logTime(getLogMessage(execFunc)) {
      createTablePlan(rel, execFunc)
    }

    tablePlan
  }

  private def convertJoinRel(rel: OlapJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      val execFunc = rel.getContext.genExecFunc(rel)

      val logicalPlan = logTime(getLogMessage(execFunc)) {
        if (execFunc == "executeMetadataQuery") {
          TableScanPlan.createMetadataTable(rel)
        } else if (execFunc == "executeSimpleAggregationQuery") {
          TableScanPlan.createSingleRow()
        } else {
          createTablePlan(rel, execFunc)
        }
      }

      logicalPlan
    } else {
      performRuntimeJoin(rel)
    }
  }

  private def getStorageType(modelStorageType: DataStorageType) = {
    if (modelStorageType.isDeltaStorage) {
      storageType = DataStorageType.DELTA
    }
    storageType
  }


  private def createTablePlan(rel: OlapRel, execFunc: String): LogicalPlan = {
    execFunc match {
      case "executeLookupTableQuery" =>
        val config = rel.getContext.getOlapSchema.getConfig
        if (config.isInternalTableEnabled) {
          TableScanPlan.createInternalTable(rel)
        } else {
          TableScanPlan.createLookupTable(rel)
        }
      case "executeOlapQuery" =>
        val storageType = getStorageType(rel.getContext.getRealization.getModel.getStorageType)
        if (storageType == DataStorageType.DELTA) {
          DeltaLakeTableScanPlan.createOlapTable(rel, filePruningMode)
        } else TableScanPlan.createOlapTable(rel)
      case "executeSimpleAggregationQuery" =>
        TableScanPlan.createSingleRow()
      case "executeMetadataQuery" =>
        TableScanPlan.createMetadataTable(rel)
      case _ =>
        // Handle unknown execFunc, or add a default case if needed
        throw new UnsupportedOperationException(s"Unsupported execFunc: $execFunc")
    }
  }

  private def performRuntimeJoin(rel: OlapJoinRel): LogicalPlan = {
    val right = stack.pollLast()
    val left = stack.pollLast()
    logTime("join") {
      plan.JoinPlan.join(Seq(left, right), rel)
    }
  }

  private def getLogMessage(execFunc: String): String = {
    execFunc match {
      case "executeLookupTableQuery" => "createLookupTable"
      case "executeOlapQuery" => "createOlapTable"
      case "executeSimpleAggregationQuery" => "createSingleRow"
      case "executeMetadataQuery" => "createMetadataTable"
      case _ => "unknownOperation"
    }
  }


  private def convertNonEquiJoinRel(rel: OlapNonEquiJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      logTime("join with table scan") {
        TableScanPlan.createOlapTable(rel)
      }
    } else {
      val right = stack.pollLast()
      val left = stack.pollLast()
      logTime("non-equi join") {
        plan.JoinPlan.nonEquiJoin(Seq.apply(left, right), rel, dataContext)
      }
    }
  }

  def cleanCache(): Unit = {
    TableScanPlan.cachePlan.get().clear()
  }

  def getResult(): LogicalPlan = {
    stack.pollLast()
  }

  private def computeFilePruningMode(): PruningMode = {
    import scala.collection.JavaConverters._

    val config = KylinConfig.getInstanceFromEnv
    val v3FileNumLimit = config.getV3FilePruningNumLimit
    val v3FileSizeLimit = config.getV3FilePruningSizeLimit

    val contextOption = ContextUtil.listContexts.asScala
      .filter(_.getStorageContext.getCandidate.getDataLayoutDetails != null)
      .filter {
        case ctx: OlapContext =>
          val fragment = ctx.getStorageContext.getCandidate.getDataLayoutDetails
          val fileNum = fragment.getNumOfFiles
          val totalSize = fragment.getSizeInBytes
          fileNum >= v3FileNumLimit || totalSize > v3FileSizeLimit
      }
      .headOption

    contextOption match {
      case Some(context) =>
        log.info("Auto set file pruning mode Cluster, kylin.query.v3.file-pruning-file-num-limit {}," +
          " kylin.query.v3.file-pruning-file-size-limit {}.", v3FileNumLimit, v3FileSizeLimit)
        FilePruningMode.CLUSTER

      case None =>
        SparderEnv.getSparkSession.sparkContext.setLocalProperty("spark.databricks.delta.stats.skipping", "false")
        log.info("Auto set file pruning mode Local, kylin.query.v3.file-pruning-file-num-limit {}," +
          " kylin.query.v3.file-pruning-file-size-limit {}.", v3FileNumLimit, v3FileSizeLimit)
        FilePruningMode.LOCAL
    }
  }
}
