/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.kylin.query.runtime

import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.TblColRef
import org.apache.kylin.query.{SchemaProcessor, UdfManager}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

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

  //  def gtSchemaToCalciteSchema(
  //                               primaryKey: ImmutableBitSet,
  //                               derivedUtil: SparderDerivedUtil,
  //                               factTableName: String,
  //                               allColumns: List[TblColRef],
  //                               sourceSchema: StructType,
  //                               gtColIdx: Array[Int],
  //                               tupleIdx: Array[Int],
  //                               topNMapping: Map[Int, Column]
  //                             ): Seq[Column] = {
  //    val gTInfoNames = SchemaProcessor.buildFactTableSortNames(sourceSchema)
  //    val calciteToGTinfo = tupleIdx.zipWithIndex.toMap
  //    var deriveMap: Map[Int, Column] = Map.empty
  //    if (derivedUtil.hasDerived) {
  //      deriveMap = derivedUtil.hostToDeriveds.flatMap { hostToDerived =>
  //        val deriveNames = derivedUtil.derivedColumnNameMapping.get(hostToDerived)
  //        val columns = mutable.ListBuffer.empty[(Int, Column)]
  //        val derivedTableName = hostToDerived.aliasTableName
  //        if (hostToDerived.deriveType.equals(DeriveType.PK_FK)) {
  //          // composite keys are split, so only copy [0] is enough,
  //          // see CubeDesc.initDimensionColumns()
  //          require(hostToDerived.calciteIdx.length == 1)
  //          require(hostToDerived.hostIdx.length == 1)
  //          val fkColumnRef = hostToDerived.join.getFKSide.getColumns.asScala.head
  //          columns.append(
  //            (
  //              hostToDerived.calciteIdx.apply(0),
  //              col(gTInfoNames.apply(hostToDerived.hostIdx.apply(0)))
  //                .alias(
  //                  SchemaProcessor
  //                    .generateDeriveTableSchemaName(
  //                      derivedTableName,
  //                      hostToDerived.derivedIndex.apply(0),
  //                      fkColumnRef.getName)
  //                    .toString)))
  //        } else {
  //          hostToDerived.calciteIdx.zip(hostToDerived.derivedIndex).foreach {
  //            case (calciteIdx, derivedIndex) =>
  //              columns.append((calciteIdx, col(deriveNames(derivedIndex))))
  //          }
  //        }
  //        columns
  //      }.toMap
  //    }
  //
  //    // may have multi TopN measures.
  //    val topNIndexs = sourceSchema.fields.map(_.dataType).zipWithIndex.filter(_._1.isInstanceOf[ArrayType])
  //    allColumns.indices
  //      .zip(allColumns)
  //      .map {
  //        case (index, column) =>
  //          var alias: String = index.toString
  //          if (column.getTableRef != null) {
  //            alias = column.getTableRef.getAlias
  //          }
  //          val columnName = "dummy_" + alias + "_" + column.getName
  //
  //          if (topNMapping.contains(index)) {
  //            topNMapping.apply(index)
  //          } else if (calciteToGTinfo.contains(index)) {
  //            val gTInfoIndex = gtColIdx.apply(calciteToGTinfo.apply(index))
  //            val hasTopN = topNMapping.nonEmpty && topNIndexs.nonEmpty
  //            if (hasTopN && topNIndexs.map(_._2).contains(gTInfoIndex)) {
  //              // topn measure will be erase when calling inline
  //              literalOne.as(s"${factTableName}_${columnName}")
  //            } else if (primaryKey.get(gTInfoIndex)) {
  //              //  primary key
  //              col(gTInfoNames.apply(gTInfoIndex))
  //            } else {
  //              //  measure
  //              col(gTInfoNames.apply(gTInfoIndex))
  //            }
  //          } else if (deriveMap.contains(index)) {
  //            deriveMap.apply(index)
  //          } else if( DataType.DATETIME_FAMILY.contains(column.getType.getName)) {
  //            // https://github.com/Kyligence/KAP/issues/14561
  //            literalTs.as(s"${factTableName}_${columnName}")
  //          } else {
  //            literalOne.as(s"${factTableName}_${columnName}")
  //          }
  //      }
  //  }
}
