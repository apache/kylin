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
package org.apache.kylin.query

import java.util.Locale

import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping
import org.apache.kylin.metadata.model.{ColumnDesc, FunctionDesc}
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._

// scalastyle:off
object SchemaProcessor {
  val COLUMN_NAME_SEPARATOR = "__"

  def buildGTSchema(
    cuboid: Cuboid,
    tableName: String): Seq[String] = {

    genColumnNames(tableName, cuboid, cuboid.getCuboidToGridTableMapping)
  }

  private def genColumnNames(tableName: String, cuboid: Cuboid, mapping: CuboidToGridTableMapping) = {
    val coolumnMapping = initColumnNameMapping(cuboid).map(_._1)
    val colAll = new ImmutableBitSet(0, mapping.getDataTypes.length)
    val measures = colAll.andNot(mapping.getPrimaryKey).asScala
    mapping.getPrimaryKey.asScala.map { i =>
      FactTableCulumnInfo(tableName, i, coolumnMapping.apply(i)).toString
    }.toSeq ++
      measures
        .map { i =>
          FactTableCulumnInfo(tableName, i, coolumnMapping.apply(i)).toString
        }
        .toSeq
  }


  def initColumnNameMapping(cuboid: Cuboid): Array[(String, String)] = {
    val cols = cuboid.getColumns.asScala.map(col =>
      (col.getIdentity.replace(".", "_"), col.getType.getName)
    ).toArray


    // "getOrderedMeasures" returns a map, need toList
    val measures = cuboid.getCubeDesc.getMeasures.asScala.toList.map(measure =>
      (measure.getName.replace(".", "_"), measure.getFunction.getReturnType)
    ).toArray

    cols ++ measures
  }

  def generateFunctionReturnDataType(function: FunctionDesc): DataType = {
    function.getExpression.toUpperCase(Locale.ROOT) match {
      case "SUM" =>
        val parameter = function.getParameter
        if (parameter.isColumnType) {
          SparkTypeUtil.toSparkType(parameter.getColRef.getType, true)
        } else {
          SparkTypeUtil.toSparkType(function.getReturnDataType, true)
        }
      case "COUNT" => LongType
      case x if x.startsWith("TOP_N") =>
        val fields = function.getParameter.getColRefs.asScala.drop(1).map(col =>
          StructField(s"DIMENSION_${col.getName}", SparkTypeUtil.toSparkType(col.getType))
        )
        DataTypes.createArrayType(StructType(Seq(
          StructField("measure", DoubleType),
          StructField("dim", StructType(fields))
        )))
      case "MAX" | "MIN" =>
        val parameter = function.getParameter
        if (parameter.isColumnType) {
          SparkTypeUtil.toSparkType(parameter.getColRef.getType)
        } else {
          SparkTypeUtil.toSparkType(function.getReturnDataType)
        }
      case "COLLECT_SET" =>
        val parameter = function.getParameter
        ArrayType(SparkTypeUtil.toSparkType(parameter.getColRef.getType))
      case _ => SparkTypeUtil.toSparkType(function.getReturnDataType)
    }
  }


  def factTableSchemaNameToColumnId(schemaName: String): Int = {
    val data = schemaName.split(COLUMN_NAME_SEPARATOR)
    data.apply(data.length - 1).toInt
  }

  def parseDeriveTableSchemaName(schemaName: String): DeriveTableColumnInfo = {
    val data = schemaName.split(COLUMN_NAME_SEPARATOR)
    try {
      DeriveTableColumnInfo(data.apply(2), data.apply(3).toInt, data.apply(1))
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def generateDeriveTableSchemaName(
    deriveTableName: String,
    colId: Int,
    columnName: String = "N"): String = {
    DeriveTableColumnInfo(deriveTableName, colId, columnName).toString
  }

  def replaceToAggravateSchemaName(
    index: Int,
    aggFuncName: String,
    hash: String,
    aggArgs: String*): String = {
    AggColumnInfo(index, aggFuncName, hash, aggArgs: _*).toString
  }

  def buildFactTableSortNames(sourceSchema: StructType): Array[String] = {
    sourceSchema.fieldNames
      .filter(name => name.startsWith("F__") || name.startsWith("R__"))
      .map(name => (factTableSchemaNameToColumnId(name), name))
      .sortBy(_._1)
      .map(_._2)
  }

  def buildSchemaWithRawTable(columnDescs: Array[ColumnDesc]): StructType = {

    StructType(columnDescs.map { columnDesc =>
      StructField(
        columnDesc.getName,
        SparkTypeUtil.toSparkType(columnDesc.getType))
    })
  }

  def genTopNSchema(
    advanceTableName: String,
    colId: Int,
    columnName: String = "N"): String = {
    TopNColumnInfo(advanceTableName, colId, columnName).toString
  }

  def createStructType(
    name: String,
    dataType: DataType,
    nullable: Boolean): StructField =
    StructField(name, dataType, nullable)
}

sealed abstract class ColumnInfo(
  tableName: String,
  columnId: Int,
  columnName: String) {
  val prefix: String

  override def toString: String =
    s"$prefix${SchemaProcessor.COLUMN_NAME_SEPARATOR}$columnName${SchemaProcessor.COLUMN_NAME_SEPARATOR}$tableName${SchemaProcessor.COLUMN_NAME_SEPARATOR}$columnId"
}

case class FactTableCulumnInfo(
  tableName: String,
  columnId: Int,
  columnName: String)
  extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "F"
}

case class DeriveTableColumnInfo(
  tableName: String,
  columnId: Int,
  columnName: String)
  extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "D"
}

case class AggColumnInfo(
  index: Int,
  funcName: String,
  hash: String,
  args: String*) {
  override def toString: String =
    s"${funcName}_${args.mkString("_")}__${index}_$hash"
}

case class TopNColumnInfo(tableName: String, columnId: Int, columnName: String)
  extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "A"
}