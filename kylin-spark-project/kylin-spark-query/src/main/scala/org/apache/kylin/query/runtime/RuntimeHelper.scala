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

import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.cube.model.CubeDesc.DeriveType
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.query.{SchemaProcessor, UdfManager}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.mutable

// scalastyle:off
object RuntimeHelper {

  final val literalOne = new Column(Literal(1, DataTypes.IntegerType))
  final val literalTs = new Column(Literal(null, DataTypes.TimestampType))

  def registerSingleByColName(funcName: String, dataType: DataType): String = {
    val name = dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
    UdfManager.register(dataType, funcName)
    name
  }

  def gtSchemaToCalciteSchema(primaryKey: ImmutableBitSet,
                              deriveSummary: DeriveSummary,
                              factTableName: String,
                              allColumns: List[TblColRef],
                              sourceSchema: StructType,
                              gtColIdx: Array[Int],
                              tupleIdx: Array[Int],
                              topNMapping: Map[Int, Column],
                              topNMeasureIndexes: Array[Int]): Seq[Column] = {
    val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
    val calciteToGTinfo = tupleIdx.zipWithIndex.toMap
    var deriveMap: Map[Int, Column] = Map.empty
    if (deriveSummary.deriveDataList.nonEmpty) {
      deriveMap = deriveSummary.deriveDataList.flatMap { hostToDerived =>
        val deriveNames = deriveSummary.deviceMapping.get(hostToDerived)
        val columns = mutable.ListBuffer.empty[(Int, Column)]
        val derivedTableName = hostToDerived.aliasTableName
        if (hostToDerived.deriveType.equals(DeriveType.PK_FK)) {
          // composite keys are split, so only copy [0] is enough,
          // see CubeDesc.initDimensionColumns()
          require(hostToDerived.calciteIdx.length == 1)
          require(hostToDerived.hostIdx.length == 1)
          val fkColumnRef = hostToDerived.join.getForeignKeyColumns
          columns.append(
            (
              hostToDerived.calciteIdx.apply(0),
              col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
                .alias(
                  SchemaProcessor
                    .generateDeriveTableSchemaName(
                      derivedTableName,
                      hostToDerived.derivedIndex.apply(0),
                      fkColumnRef(0).getName)
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
            val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
            val hasTopN = topNMapping.nonEmpty && topNMeasureIndexes.nonEmpty
            if (hasTopN && topNMeasureIndexes.contains(gTInfoIndex)) {
              // topn measure will be erase when calling inline
              literalOne.as(s"${factTableName}_${columnName}")
            } else if (primaryKey.get(gTInfoIndex)) {
              //  primary key
              col(gTInfoNames.apply(gTInfoIndex))
            } else {
              //  measure
              col(gTInfoNames.apply(gTInfoIndex))
            }
          } else if (deriveMap.contains(index)) {
            deriveMap.apply(index)
          } else if (DataType.DATETIME_FAMILY.contains(column.getType.getName)) {
            literalTs.as(s"${factTableName}_${columnName}")
          } else {
            literalOne.as(s"${factTableName}_${columnName}")
          }
      }
  }
}
