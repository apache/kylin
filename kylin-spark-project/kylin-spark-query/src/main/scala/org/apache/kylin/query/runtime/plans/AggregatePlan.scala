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
package org.apache.kylin.query.runtime.plans

import org.apache.calcite.DataContext
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.sql.SqlKind
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.metadata.model.{FunctionDesc, PartitionDesc, SegmentStatusEnum}
import org.apache.kylin.query.SchemaProcessor
import org.apache.kylin.query.relnode.{KylinAggregateCall, OLAPAggregateRel, OLAPRel}
import org.apache.kylin.query.runtime.RuntimeHelper
import org.apache.spark.sql.KylinFunctions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{CreateArray, In}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.utils.LogEx

import scala.collection.JavaConverters._

// scalastyle:off
object AggregatePlan extends LogEx {
  val binaryMeasureType =
    List("PERCENTILE", "PERCENTILE_APPROX", "INTERSECT_COUNT", "COUNT_DISTINCT")

  def agg(
    inputs: java.util.List[DataFrame],
    rel: OLAPAggregateRel,
    dataContext: DataContext): DataFrame = logTime("aggregate", info = true) {

    var dataFrame = inputs.get(0)
    val schemaNames = dataFrame.schema.fieldNames
    val groupList = rel.getGroupSet.asScala
      .map(groupId => col(schemaNames.apply(groupId)))
      .toList

    rel.getContext.isExactlyAggregate = isExactlyCuboidMatched(rel, groupList)
    if (rel.getContext.isExactlyAggregate) {
      // exactly match, skip agg, direct project.
      val hash = System.identityHashCode(rel).toString
      val aggCols = rel.getRewriteAggCalls.asScala.zipWithIndex.map {
        case (call: KylinAggregateCall, index: Int) =>
          val dataType = call.getFunc.getReturnDataType
          val funcName = OLAPAggregateRel.getAggrFuncName(call)
          val argNames = call.getArgList.asScala.map(dataFrame.schema.names.apply(_))
          val columnName = argNames.map(col)
          val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, funcName, hash, argNames: _*)
          funcName match {
            case FunctionDesc.FUNC_COUNT_DISTINCT =>
              if (call.isHllCountDistinctFunc) {
                KylinFunctions
                  .approx_count_distinct_decode(columnName.head, dataType.getPrecision)
                  .alias(aggName)
              } else if (call.isBitmapCountDistinctFunc) {
                // execute count distinct precisely
                KylinFunctions.precise_count_distinct_decode(columnName.head).alias(aggName)
              } else {
                throw new IllegalArgumentException(
                  s"""Unsupported function name $funcName""")
              }
            case FunctionDesc.FUNC_PERCENTILE =>
              val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "PERCENTILE_DECODE", hash, argNames: _*)
              KylinFunctions.k_percentile_decode(columnName.head, columnName(1), dataType.getPrecision).alias(aggName)
            case _ =>
              col(schemaNames.apply(call.getArgList.get(0)))
          }
        case (call: Any, index: Int) =>
          col(schemaNames.apply(call.getArgList.get(0)))
      }.toList
      val prjList = groupList ++ aggCols
      logInfo(s"Query exactly match index, skip agg, project $prjList.")
      dataFrame.select(prjList: _*)
    } else {
      dataFrame = genFiltersWhenIntersectCount(rel, dataFrame)
      val aggList = buildAgg(dataFrame.schema, rel)
      val groupSets = rel.getGroupSets.asScala
        .map(groupSet => groupSet.asScala.map(groupId => col(schemaNames.apply(groupId))).toList)
        .toList
      SparkOperation.agg(AggArgc(dataFrame, groupList, aggList, groupSets,
        rel.getGroupType() == Aggregate.Group.SIMPLE))
    }
  }

  private def genFiltersWhenIntersectCount(rel: OLAPAggregateRel, dataFrame: DataFrame): DataFrame = {
    try {
      val intersects = rel.getRewriteAggCalls.asScala.filter(_.isInstanceOf[KylinAggregateCall])
        .filter(!_.asInstanceOf[KylinAggregateCall].getFunc.isCount)
        .map(_.asInstanceOf[KylinAggregateCall])
        .filter(call => !call.getFunc.isCount && OLAPAggregateRel.getAggrFuncName(call).equals(FunctionDesc.FUNC_INTERSECT_COUNT))
      val names = dataFrame.schema.names
      val children = dataFrame.queryExecution.logical
      if (intersects.nonEmpty && intersects.size == rel.getRewriteAggCalls.size() && children.isInstanceOf[Project]) {
        val list = children.asInstanceOf[Project].projectList
        val filters = intersects.map { call =>
          val filterColumnIndex = call.getArgList.get(1)
          val litIndex = call.getArgList.get(2)
          new Column(In(col(names(filterColumnIndex)).expr, list.apply(litIndex).children.head.asInstanceOf[CreateArray].children))
        }
        val column = filters.reduceLeft(_.or(_))
        dataFrame.filter(column)
      } else {
        dataFrame
      }
    } catch {
      case e: Throwable => logWarning("Error occurred when generate filters", e)
        dataFrame
    }
  }

  def buildAgg(
    schema: StructType,
    rel: OLAPAggregateRel): List[Column] = {
    val hash = System.identityHashCode(rel).toString

    rel.getRewriteAggCalls.asScala.zipWithIndex.map {
      case (call: KylinAggregateCall, index: Int)
        if binaryMeasureType.contains(OLAPAggregateRel.getAggrFuncName(call)) =>
        val dataType = call.getFunc.getReturnDataType
        val isCount = call.getFunc.isCount
        val funcName =
          if (isCount) FunctionDesc.FUNC_COUNT else OLAPAggregateRel.getAggrFuncName(call)
        val argNames = call.getArgList.asScala.map(schema.names.apply(_))
        val columnName = argNames.map(col)
        val registeredFuncName = RuntimeHelper.registerSingleByColName(funcName, dataType)
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, funcName, hash, argNames: _*)
        if (funcName == FunctionDesc.FUNC_COUNT_DISTINCT) {
          if (call.isHllCountDistinctFunc) {
            KylinFunctions
              .approx_count_distinct(columnName.head, dataType.getPrecision)
              .alias(aggName)
          } else if (call.isBitmapCountDistinctFunc) {
            // execute count distinct precisely
            KylinFunctions.precise_count_distinct(columnName.head).alias(aggName)
          } else {
            // for intersect_count and intersect_value function
            require(columnName.size == 3, s"Input columns size ${columnName.size} don't equal to 3.")
            val columns = columnName.zipWithIndex.map {
              case (column: Column, 2) => column.cast(ArrayType.apply(schema.fields.apply(call.getArgList.get(1)).dataType))
              case (column: Column, _) => column
            }
            val upperBound = KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound
            if (call.isIntersectCountFunc) {
              KylinFunctions.intersect_count(upperBound, columns.toList: _*)
                .alias(SchemaProcessor
                  .replaceToAggravateSchemaName(index, FunctionDesc.FUNC_INTERSECT_COUNT, hash,
                    argNames: _*))
            } else {
              KylinFunctions.intersect_value(upperBound, columns.toList: _*)
                .alias(SchemaProcessor
                  .replaceToAggravateSchemaName(index, FunctionDesc.FUNC_INTERSECT_VALUE, hash,
                    argNames: _*))
            }
          }
        } else {
          callUDF(registeredFuncName, columnName.toList: _*).alias(aggName)
        }
      case (call: Any, index: Int) =>
        val funcName = OLAPAggregateRel.getAggrFuncName(call)
        val schemaNames = schema.names
        val argNames = call.getArgList.asScala.map(id => schemaNames.apply(id))
        val inputType = call.getType
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index,
          funcName,
          hash,
          argNames: _*)
        funcName match {
          case FunctionDesc.FUNC_SUM =>
            if (isSum0(call)) {
              sum0(
                col(argNames.head).cast(
                  SparkTypeUtil.convertSqlTypeToSparkType(inputType)))
                .alias(aggName)
            } else {
              sum(
                col(argNames.head).cast(
                  SparkTypeUtil.convertSqlTypeToSparkType(inputType)))
                .alias(aggName)
            }
          case FunctionDesc.FUNC_COUNT =>
            count(if (argNames.isEmpty) k_lit(1) else col(argNames.head))
              .alias(aggName)
          case FunctionDesc.FUNC_MAX =>
            max(
              col(argNames.head).cast(
                SparkTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_MIN =>
            min(
              col(argNames.head).cast(
                SparkTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_COUNT_DISTINCT =>
            countDistinct(argNames.head, argNames.drop(1): _*)
              .alias(aggName)
          // Issue 4337: Supported select (select '2012-01-02') as data, xxx from table group by xxx
          case SqlKind.SINGLE_VALUE.sql =>
            first(argNames.head).alias(aggName)
          case FunctionDesc.FUNC_GROUPING =>
            grouping(argNames.head).alias(aggName)
          case FunctionDesc.FUNC_PERCENTILE => {
            val col = argNames(0)
            val inputColumnRowType = rel.getInput.asInstanceOf[OLAPRel].getColumnRowType
            val percentage = inputColumnRowType.getColumnByIndex(call.getArgList.get(1)).getName
            expr(s"approx_percentile($col, $percentage)").alias(aggName)
          }

          case _ =>
            throw new IllegalArgumentException(
              s"""Unsupported function name $funcName""")
        }
    }.toList
  }

  private def isSum0(call: AggregateCall) = {
    call.isInstanceOf[KylinAggregateCall] && call
      .asInstanceOf[KylinAggregateCall]
      .isSum0
  }

  val exactlyMatchSupportedFunctions = List("SUM", "MIN", "MAX", "COUNT_DISTINCT", "PERCENTILE", "PERCENTILE_APPROX")

  def isExactlyCuboidMatched(rel: OLAPAggregateRel, groupByList: List[Column]): Boolean = {
    val olapContext = rel.getContext
    if (olapContext == null || olapContext.realization == null) return false
    if (!olapContext.realization.getConfig.needReplaceAggWhenExactlyMatched) return false

    val cuboid = olapContext.storageContext.getCuboid
    if (cuboid == null) return false
    if (olapContext.hasJoin) return false
    // when querying with group by grouping sets, cube, rollup
    if (rel.getGroupType() != Aggregate.Group.SIMPLE) return false

    for (call <- rel.getRewriteAggCalls.asScala) {
      if (!exactlyMatchSupportedFunctions.contains(OLAPAggregateRel.getAggrFuncName(call))) {
        return false
      }
      // when using intersect_count and intersect_value
      if (call.getArgList.size() > 1 && !OLAPAggregateRel.getAggrFuncName(call).startsWith("PERCENTILE")) {
        return false
      }
    }
    val groupByCols = rel.getGroups.asScala.map(_.getIdentity).toSet
    if (groupByCols.isEmpty) return false
    if (!groupByContainsPartition(groupByCols, cuboid.getCubeDesc.getModel.getPartitionDesc) &&
      olapContext.realization.asInstanceOf[CubeInstance].getSegments(SegmentStatusEnum.READY).size() != 1) {
      return false
    }
    val cuboidDims = cuboid.getColumns.asScala.map(_.getIdentity).toSet
    groupByCols.equals(cuboidDims)
  }

  def groupByContainsPartition(groupByCols: Set[String], partitionDesc: PartitionDesc): Boolean = {
    partitionDesc != null && partitionDesc.getPartitionDateColumnRef != null && groupByCols.contains(partitionDesc.getPartitionDateColumnRef.getIdentity)
  }
}
