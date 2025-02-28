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
package org.apache.kylin.query.runtime.plan

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.sql.SqlKind
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.measure.percentile.PercentileCounter
import org.apache.kylin.metadata.model.FunctionDesc
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem
import org.apache.kylin.query.relnode.{KylinAggregateCall, OlapAggregateRel, OlapProjectRel}
import org.apache.kylin.query.util.RuntimeHelper
import org.apache.spark.sql.KapFunctions.{k_lit, sum0}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.catalyst.expressions.{Attribute, CreateArray, In}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.udaf.{BitmapFuncType, SingleValueAgg}
import org.apache.spark.sql.util.SparderTypeUtil

import java.util.Locale
import scala.collection.JavaConverters._

// scalastyle:off
object AggregatePlan extends LogEx {
  val unSupportedOperationEp = s"limit / offset must be of constant literal"
  val binaryMeasureType =
    List("PERCENTILE", "PERCENTILE_APPROX", "INTERSECT_COUNT", "COUNT_DISTINCT", "BITMAP_UUID",
      FunctionDesc.FUNC_BITMAP_BUILD, FunctionDesc.FUNC_SUM_LC)

  def agg(plan: LogicalPlan,
          rel: OlapAggregateRel): LogicalPlan = logTime("aggregate", debug = true) {

    val schemaNames = plan.output

    val groupList = rel.getRewriteGroupKeys.asScala.map(groupId => col(schemaNames.apply(groupId).name)).toList

    if (rel.getContext != null && rel.getContext.isExactlyAggregate && !rel.getContext.isNeedToManyDerived) {
      // exactly match, skip agg, direct project.
      val aggCols = rel.getRewriteAggCalls.asScala.zipWithIndex.map {
        case (call: KylinAggregateCall, index: Int) =>
          val funcName = OlapAggregateRel.getAggrFuncName(call);
          val dataType = call.getFunc.getReturnDataType
          val argNames = call.getArgList.asScala.map(schemaNames.apply(_).name)
          val columnName = argNames.map(name => col(name))
          val hash = System.identityHashCode(rel).toString
          funcName match {
            case FunctionDesc.FUNC_COUNT_DISTINCT =>
              if (call.isHllCountDistinctFunc) {
                val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "APPROX_COUNT_DISTINCT_DECODE", hash, argNames: _*)
                KapFunctions.approx_count_distinct_decode(columnName.head, dataType.getPrecision).alias(aggName)
              } else if (call.isBitmapCountDistinctFunc) {
                if (rel.getContext.isExactlyFastBitmap) {
                  col(schemaNames.apply(call.getArgList.get(0)).name)
                } else {
                  val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "PRECISE_COUNT_DISTINCT_DECODE", hash, argNames: _*)
                  KapFunctions.precise_count_distinct_decode(columnName.head).alias(aggName)
                }
              } else {
                throw new IllegalArgumentException(
                  s"""Unsupported function name $funcName""")
              }
            case FunctionDesc.FUNC_PERCENTILE =>
              val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "PERCENTILE_DECODE", hash, argNames: _*)
              KapFunctions.k_percentile_decode(columnName.head, columnName(1), dataType.getPrecision).alias(aggName)
            case FunctionDesc.FUNC_BITMAP_BUILD =>
              val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "BITMAP_BUILD_DECODE", hash, argNames: _*)
              KapFunctions.precise_bitmap_build_decode(columnName.head).alias(aggName)
            case FunctionDesc.FUNC_SUM_LC =>
              val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, "SUM_LC_DECODE", hash, argNames: _*)
              val sparkDataType = SparderTypeUtil.toSparkType(dataType)
              KapFunctions.k_sum_lc_decode(columnName.head, sparkDataType.json).alias(aggName)
            case _ =>
              col(schemaNames.apply(call.getArgList.get(0)).name)
          }
        case (call: Any, _: Int) =>
          col(schemaNames.apply(call.getArgList.get(0)).name)
      }.toList

      val prjList = groupList ++ aggCols
      logInfo(s"Query exactly match index, skip agg, project $prjList.")
      SparkOperation.project(prjList, plan)
    } else {
      val intersectplan = genFiltersWhenIntersectCount(rel, plan)
      val aggList = buildAgg(intersectplan.output, rel, plan)
      val groupSets = rel.getRewriteGroupSets.asScala
        .map(groupSet => groupSet.asScala.map(groupId => col(schemaNames.apply(groupId).name)).toList).toList
      SparkOperation.agg(AggArgc(intersectplan, groupList, aggList, groupSets, rel.isSimpleGroupType))
    }
  }

  private def genFiltersWhenIntersectCount(rel: OlapAggregateRel, plan: LogicalPlan): LogicalPlan = {
    try {
      val names = plan.output

      val intersects = rel.getRewriteAggCalls.asScala.filter(_.isInstanceOf[KylinAggregateCall])
        .filter(!_.asInstanceOf[KylinAggregateCall].getFunc.isCount)
        .map(_.asInstanceOf[KylinAggregateCall])
        .filter(call => !call.getFunc.isCount && OlapAggregateRel.getAggrFuncName(call).equals(FunctionDesc.FUNC_INTERSECT_COUNT))
      val children = plan
      if (intersects.nonEmpty && intersects.size == rel.getRewriteAggCalls.size() && children.isInstanceOf[Project]) {
        // only exists intersect count function in agg
        val list = children.asInstanceOf[Project].projectList
        val supportGenFilter = intersects.forall { call =>
          val listIndex = call.getArgList.get(2)
          call.getArgList.size() == 3 && list.apply(listIndex).eval().asInstanceOf[ArrayData].array.map(_.toString)
            .forall(!_.contains(KylinConfig.getInstanceFromEnv.getIntersectFilterOrSeparator))
        }
        if (supportGenFilter) {
          val filters = intersects.map { call =>
            val filterColumnIndex = call.getArgList.get(1)
            val litIndex = call.getArgList.get(2)
            new Column(In(col(names(filterColumnIndex).name).expr, list.apply(litIndex).children.head.asInstanceOf[CreateArray].children))
          }
          val column = filters.reduceLeft(_.or(_))

          val filterPlan = Filter(column.expr, plan)
          SparkOperation.project(plan.output.map(c => col(c.name)), filterPlan)
        } else {
          plan
        }
      } else {
        plan
      }
    } catch {
      case e: Throwable => logWarning("Error occurred when generate filters", e)
        plan
    }
  }

  // S53 Fix https://olapio.atlassian.net/browse/KE-42473
  def buildAgg(schema: Seq[Attribute],
               rel: OlapAggregateRel,
               plan: LogicalPlan): List[Column] = {
    val hash = System.identityHashCode(rel).toString

    rel.getRewriteAggCalls.asScala.zipWithIndex.map {
      case (call: KylinAggregateCall, index: Int)
        if binaryMeasureType.contains(OlapAggregateRel.getAggrFuncName(call)) =>
        val dataType = call.getFunc.getReturnDataType
        val isCount = call.getFunc.isCount
        val funcName =
          if (isCount) FunctionDesc.FUNC_COUNT else OlapAggregateRel.getAggrFuncName(call)
        val argNames = call.getArgList.asScala.map(schema.apply(_).name)
        val columnName = argNames.map(name => col(name))
        val registeredFuncName = RuntimeHelper.registerSingleByColName(funcName, dataType)
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index, funcName, hash, argNames: _*)
        if (funcName == FunctionDesc.FUNC_COUNT_DISTINCT) {
          if (dataType.getName == "hllc") {
            org.apache.spark.sql.KapFunctions
              .approx_count_distinct(columnName.head, dataType.getPrecision)
              .alias(aggName)
          } else {
            KapFunctions.precise_count_distinct(columnName.head).alias(aggName)
          }
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_BITMAP_UUID)) {
          KapFunctions.precise_bitmap_uuid(columnName.head).alias(aggName)
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_BITMAP_BUILD)) {
          KapFunctions.precise_bitmap_build(columnName.head).alias(aggName)
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_INTERSECT_COUNT)) {
          require(columnName.size >= 3, s"Input columns size ${columnName.size} don't greater than or equal to 3.")
          val resolvedPlan = SparkInternalAgent.getDataFrame(SparderEnv.getSparkSession, plan)
          val columns = columnName.slice(0, 3).zipWithIndex.map {
            case (column: Column, 2) => column.cast(ArrayType.apply(resolvedPlan.schema.apply(call.getArgList.get(1)).dataType))
            case (column: Column, _) => column
          }
          val separator = s"\\${KylinConfig.getInstanceFromEnv.getIntersectFilterOrSeparator}"
          val upperBound = KylinConfig.getInstanceFromEnv.getBitmapValuesUpperBound
          call.name.toUpperCase(Locale.ROOT) match {
            case FunctionDesc.FUNC_INTERSECT_COUNT => KapFunctions.intersect_count(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_VALUE => KapFunctions.intersect_value(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID => KapFunctions.intersect_bitmap(separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_COUNT_V2 => KapFunctions.intersect_count_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_VALUE_V2 => KapFunctions.intersect_value_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_V2 => KapFunctions.intersect_bitmap_v2(columnName.last, separator, upperBound, columns.toList: _*).alias(aggName)
            case func => throw new UnsupportedOperationException(s"Unsupported intersect count function: $func, please check the sql.")
          }
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_PERCENTILE)) {
          require(columnName.size == 2, s"Input columns size ${columnName.size} don't equal to 2.")
          KapFunctions.k_percentile(columnName.head, columnName(1), dataType.getPrecision).alias(aggName)
        } else if (funcName.equalsIgnoreCase(FunctionDesc.FUNC_SUM_LC)) {
          KapFunctions.k_sum_lc(columnName.head, SparderTypeUtil.toSparkType(dataType)).alias(aggName)
        } else {
          callUDF(registeredFuncName, columnName.toList: _*).alias(aggName)
        }
      case (call: Any, index: Int) =>
        val funcName = OlapAggregateRel.getAggrFuncName(call)
        val argNames = call.getArgList.asScala.map(id => schema.apply(id).name)
        val columnName = argNames.map(name => col(name))
        val inputType = call.getType
        val aggName = SchemaProcessor.replaceToAggravateSchemaName(index,
          funcName,
          hash,
          argNames: _*)
        funcName match {
          case FunctionDesc.FUNC_PERCENTILE =>
            rel.getInput match {
              case projectRel: OlapProjectRel =>
                val percentageArg = projectRel.getProjects.get(call.getArgList.get(1))
                val accuracyArg = if (call.getArgList.size() < 3) {
                  None
                } else {
                  Some(projectRel.getProjects.get(call.getArgList.get(2)))
                }
                (percentageArg, accuracyArg) match {
                  case (percentageLitRex: RexLiteral, accuracyArgLitRex: Option[RexLiteral]) =>
                    if (KylinConfig.getInstanceFromEnv.getPercentileApproxAlgorithm.equalsIgnoreCase("t-digest")) {
                      KapFunctions.k_percentile(columnName.head, columnName(1), PercentileCounter.DEFAULT_PERCENTILE_ACCURACY).alias(aggName)
                    } else {
                      val percentage = percentageLitRex.getValue
                      val accuracy = accuracyArgLitRex.map(arg => arg.getValue).getOrElse(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
                      percentile_approx(col(argNames.head), lit(percentage), lit(accuracy)).alias(aggName)
                    }
                }
              case _ =>
                throw new UnsupportedOperationException(s"Invalid percentile_approx parameters, " +
                  s"expecting approx_percentile(col, percentage [, accuracy]), percentage/accuracy must be of constant literal")
            }
          case FunctionDesc.FUNC_SUM =>
            // TODO Use improved sum decimal precision by default in future
            if (KylinRelDataTypeSystem.getProjectConfig.isImprovedSumDecimalPrecisionEnabled) {
              if (isSum0(call)) {
                sum0(col(argNames.head))
                  .cast(SparderTypeUtil.convertSqlTypeToSparkType(inputType))
                  .alias(aggName)
              } else {
                sum(col(argNames.head))
                  .cast(SparderTypeUtil.convertSqlTypeToSparkType(inputType))
                  .alias(aggName)
              }
            } else {
              if (isSum0(call)) {
                sum0(
                  col(argNames.head).cast(
                    SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
                  .alias(aggName)
              } else {
                sum(
                  col(argNames.head).cast(
                    SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
                  .alias(aggName)
              }
            }
          case FunctionDesc.FUNC_COUNT =>
            count(if (argNames.isEmpty) k_lit(1) else col(argNames.head))
              .alias(aggName)
          case FunctionDesc.FUNC_MAX =>
            max(
              col(argNames.head).cast(
                SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_MIN =>
            min(
              col(argNames.head).cast(
                SparderTypeUtil.convertSqlTypeToSparkType(inputType)))
              .alias(aggName)
          case FunctionDesc.FUNC_COUNT_DISTINCT if call.getAggregation.getName == "BITMAP_COUNT" =>
            KapFunctions.precise_count_distinct(col(argNames.head)).alias(aggName)
          case FunctionDesc.FUNC_COUNT_DISTINCT =>
            countDistinct(argNames.head, argNames.drop(1): _*)
              .alias(aggName)
          case FunctionDesc.FUNC_BITMAP_BUILD =>
            KapFunctions.precise_bitmap_build_pushdown(columnName.head).alias(aggName)
          // Issue 4337: Supported select (select '2012-01-02') as data, xxx from table group by xxx
          case SqlKind.SINGLE_VALUE.sql =>
            val structField = StructField(schema.head.name, SparderTypeUtil.convertSqlTypeToSparkType(inputType), true, Metadata.empty)
            SingleValueAgg(structField).apply(col(argNames.head)).alias(aggName)
          case FunctionDesc.FUNC_GROUPING =>
            if (!rel.isSimpleGroupType) {
              grouping(argNames.head).alias(aggName)
            } else {
              if (rel.getRewriteGroupKeys.contains(call.getArgList.get(0))) {
                k_lit(0).alias(aggName)
              } else {
                k_lit(1).alias(aggName)
              }
            }
          case FunctionDesc.FUNC_COLLECT_SET =>
            call match {
              case kac: KylinAggregateCall =>
                array_distinct(flatten(collect_set(col(argNames.head))))
                  .alias(aggName)
              case _ =>
                collect_set(col(argNames.head)).alias(aggName)
            }
          case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_DISTINCT =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), BinaryType, BitmapFuncType.INTERSECT).alias(aggName)
          case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_COUNT =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), IntegerType, BitmapFuncType.INTERSECT).alias(aggName)
          case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE =>
            rel.getInput match {
              case projectRel: OlapProjectRel =>
                val limitArg = projectRel.getProjects.get(call.getArgList.get(1))
                val offsetArg = projectRel.getProjects.get(call.getArgList.get(2))
                (limitArg, offsetArg) match {
                  case (limitLit: RexLiteral, offsetLit: RexLiteral) =>
                    val limit = limitLit.getValue.toString.toInt
                    val offset = offsetLit.getValue.toString.toInt
                    KapFunctions.bitmap_uuid_page_func(col(argNames.head), limit, offset, ArrayType(LongType, false),
                      BitmapFuncType.INTERSECT).alias(aggName)
                  case _ =>
                    throw new UnsupportedOperationException()
                }
              case _ =>
                throw new UnsupportedOperationException(unSupportedOperationEp)
            }
          case FunctionDesc.FUNC_INTERSECT_BITMAP_UUID_VALUE_ALL =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), ArrayType(LongType, false), BitmapFuncType.INTERSECT).alias(aggName)
          case FunctionDesc.FUNC_UNION_BITMAP_UUID_DISTINCT =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), BinaryType, BitmapFuncType.UNION).alias(aggName)
          case FunctionDesc.FUNC_UNION_BITMAP_UUID_COUNT =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), IntegerType, BitmapFuncType.UNION).alias(aggName)
          case FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE =>
            rel.getInput match {
              case projectRel: OlapProjectRel =>
                val limitArg = projectRel.getProjects.get(call.getArgList.get(1))
                val offsetArg = projectRel.getProjects.get(call.getArgList.get(2))
                (limitArg, offsetArg) match {
                  case (limitLit: RexLiteral, offsetLit: RexLiteral) =>
                    val limit = limitLit.getValue.toString.toInt
                    val offset = offsetLit.getValue.toString.toInt
                    KapFunctions.bitmap_uuid_page_func(col(argNames.head), limit, offset, ArrayType(LongType, false),
                      BitmapFuncType.UNION).alias(aggName)
                  case _ =>
                    throw new UnsupportedOperationException(unSupportedOperationEp)
                }
              case _ =>
                throw new UnsupportedOperationException(unSupportedOperationEp)
            }
          case FunctionDesc.FUNC_UNION_BITMAP_UUID_VALUE_ALL =>
            KapFunctions.bitmap_uuid_func(col(argNames.head), ArrayType(LongType, false), BitmapFuncType.UNION).alias(aggName)
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
}
