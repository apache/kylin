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

package org.apache.kylin.query.util

import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.DeriveInfo.DeriveType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import org.apache.spark.sql.udf.UdfManager

import scala.collection.JavaConverters._
import scala.collection.mutable

// scalastyle:off
object RuntimeHelper extends Logging {

  final val intZero = new Column(Literal(0, DataTypes.IntegerType))
  final val intOne = new Column(Literal(1, DataTypes.IntegerType))
  final val timestampNull = new Column(Literal(null, DataTypes.TimestampType))
  final val stringNull = new Column(Literal(null, DataTypes.StringType))
  final val boolNull = new Column(Literal(null, DataTypes.BooleanType))

  def registerSingleByColName(funcName: String, dataType: DataType): String = {
    val name = dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
    UdfManager.register(dataType, funcName)
    name
  }

  // S53 Fix https://olapio.atlassian.net/browse/KE-42473
  def gtSchemaToCalciteSchema(primaryKey: ImmutableBitSet,
                              derivedUtil: SparderDerivedUtil,
                              factTableName: String,
                              allColumns: List[TblColRef],
                              plan: LogicalPlan,
                              colIdx: (Array[Int], Array[Int]),
                              topNMapping: Map[Int, Column]): Seq[Column] = {
    val gtColIdx = colIdx._1
    val tupleIdx = colIdx._2
    val sourceSchema = plan.output
    val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
    val calciteToGTinfo = tupleIdx.zipWithIndex.toMap
    var deriveMap: Map[Int, Column] = Map.empty
    if (derivedUtil.hasDerived) {
      deriveMap = derivedUtil.hostToDeriveds.flatMap { hostToDerived =>
        val deriveNames = derivedUtil.derivedColumnNameMapping.get(hostToDerived)
        val columns = mutable.ListBuffer.empty[(Int, Column)]
        val derivedTableName = hostToDerived.aliasTableName
        if (DeriveType.PK_FK == hostToDerived.deriveType) {
          // composite keys are split, so only copy [0] is enough,
          // see CubeDesc.initDimensionColumns()
          require(hostToDerived.calciteIdx.length == 1)
          require(hostToDerived.hostIdx.length == 1)
          val fkColumnRef = hostToDerived.join.getFKSide.getColumns.asScala.head
          columns.append(
            (
              hostToDerived.calciteIdx.apply(0),
              col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
                .alias(
                  SchemaProcessor
                    .generateDeriveTableSchemaName(
                      derivedTableName,
                      hostToDerived.derivedIndex.apply(0),
                      fkColumnRef.getName)
                    .toString)))
        } else {
          hostToDerived.calciteIdx.zip(hostToDerived.derivedIndex).foreach {
            case (calciteIdx, derivedIndex) =>
              columns.append((calciteIdx, col(deriveNames(derivedIndex))))
          }
        }
        columns
      }.toMap
    }

    val projectConfig = NProjectManager.getProjectConfig(derivedUtil.model.getProject)
    // may have multi TopN measures.
    val topNIndexs = if (plan.resolved) {
      sourceSchema.map(_.dataType).zipWithIndex.filter(_._1.isInstanceOf[ArrayType])
    } else {
      Seq.empty
    }

    allColumns.indices
      .zip(allColumns)
      .map {
        case (index, column) =>
          var alias: String = index.toString
          if (column.getTableRef != null) {
            alias = column.getTableRef.getAlias
          }
          val columnName = "dummy_" + alias + "_" + column.getName

          if (topNMapping.contains(index)) {
            topNMapping.apply(index)
          } else if (calciteToGTinfo.contains(index)) {
            try {
              val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
              val hasTopN = topNMapping.nonEmpty && topNIndexs.nonEmpty
              if (hasTopN && topNIndexs.map(_._2).contains(gTInfoIndex)) {
                // topn measure will be erase when calling inline
                intOne.as(s"${factTableName}_${columnName}")
              } else if (projectConfig.useTableIndexAnswerSelectStarEnabled() && gTInfoIndex < 0) {
                if (column.getColumnDesc.getType.isNumberFamily) {
                  intZero.as(s"${factTableName}_${columnName}")
                } else {
                  stringNull.as(s"${factTableName}_${columnName}")
                }
              } else if (primaryKey.get(gTInfoIndex)) {
                //  primary key
                col(gTInfoNames.apply(gTInfoIndex))
              } else {
                //  measure
                col(gTInfoNames.apply(gTInfoIndex))
              }
            } catch {
              case e: IndexOutOfBoundsException => {
                logWarning("Column found lost in layout columnName: " + columnName +
                  ", allColumns: " + allColumns.toString(), e)
                throw e
              }
            }
          } else if (deriveMap.contains(index)) {
            deriveMap.apply(index)
          } else if (DataType.DATETIME_FAMILY.contains(column.getType.getName)) {
            // https://github.com/Kyligence/KAP/issues/14561
            timestampNull.as(s"${factTableName}_${columnName}")
          } else if (DataType.STRING_FAMILY.contains(column.getType.getName)) {
            stringNull.as(s"${factTableName}_${columnName}")
          } else if (DataType.BOOLEAN_FAMILY.contains(column.getType.getName)) {
            boolNull.as(s"${factTableName}_${columnName}")
          } else {
            intOne.as(s"${factTableName}_${columnName}")
          }
      }
  }
}
