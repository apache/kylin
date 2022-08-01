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
package org.apache.spark.sql.execution.utils

import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.common.{KapConfig, KylinConfig}

import java.util
import org.apache.kylin.metadata.cube.gridtable.NLayoutToGridTableMapping
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataSegment, NDataflow, NDataflowManager}
import org.apache.kylin.query.runtime.plan.TableScanPlan
import org.apache.kylin.metadata.model.{ColumnDesc, FunctionDesc}
import org.apache.spark.sql.{LayoutEntityConverter, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderConstants.COLUMN_NAME_SEPARATOR
import org.apache.spark.sql.util.{SparderConstants, SparderTypeUtil}

import scala.collection.JavaConverters._

// scalastyle:off
object SchemaProcessor {

  def buildGTSchema(cuboid: LayoutEntity,
                    mapping: NLayoutToGridTableMapping,
                    tableName: String):Seq[String] = {

    genColumnNames(tableName, cuboid, mapping)
  }

  private def genColumnNames(tableName: String, cuboid: LayoutEntity, mapping: NLayoutToGridTableMapping) = {
    val columnMapping = initColumnNameMapping(cuboid).map(_._1)
    val colAll = new ImmutableBitSet(0, mapping.getDataTypes.length)
    val measures = colAll.andNot(mapping.getPrimaryKey).asScala
    mapping.getPrimaryKey.asScala.map { i =>
      FactTableCulumnInfo(tableName, i, columnMapping.apply(i)).toString
    }.toSeq ++
      measures
        .map { i =>
          FactTableCulumnInfo(tableName, i, columnMapping.apply(i)).toString
        }
        .toSeq
  }

  def initColumnNameMapping(cuboid: LayoutEntity): Array[(String, String)] = {
    val cols = cuboid.getColumns.asScala.map(col =>
      (col.getIdentity.replace(".", "_"), col.getType.getName)
    ).toArray

    // "getOrderedMeasures" returns a map, need toList
    val measures = cuboid.getOrderedMeasures.asScala.toList.map(measure =>
      (measure._2.getName.replace(".", "_"), measure._2.getFunction.getReturnType)
    ).toArray

    cols ++ measures
  }

  def generateFunctionReturnDataType(function: FunctionDesc): DataType = {
    function.getExpression.toUpperCase match {
      case "SUM" =>
        val parameter = function.getParameters.get(0)
        if (parameter.isColumnType) {
          SparderTypeUtil.toSparkType(parameter.getColRef.getType, true)
        } else {
          SparderTypeUtil.toSparkType(function.getReturnDataType, true)
        }
      case "COUNT" => LongType
      case x if x.startsWith("TOP_N")  =>
        val fields = function.getParameters.asScala.drop(1).map(p =>
          StructField(s"DIMENSION_${p.getColRef.getName}", SparderTypeUtil.toSparkType(p.getColRef.getType))
        )
        DataTypes.createArrayType(StructType(Seq(
          StructField("measure", DoubleType),
          StructField("dim", StructType(fields))
        )))
      case "MAX" | "MIN" =>
        val parameter = function.getParameters.get(0)
        if (parameter.isColumnType) {
          SparderTypeUtil.toSparkType(parameter.getColRef.getType)
        } else {
          SparderTypeUtil.toSparkType(function.getReturnDataType)
        }
      case "COLLECT_SET" =>
        val parameter = function.getParameters.get(0)
        ArrayType(SparderTypeUtil.toSparkType(parameter.getColRef.getType))
      case _ =>  SparderTypeUtil.toSparkType(function.getReturnDataType)
    }
  }

  def checkSchema(sparkSession: SparkSession, dfName: String, project: String): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    val df: NDataflow = dsMgr.getDataflow(dfName)
    val latestReadySegment: NDataSegment = df.getQueryableSegments.getFirstSegment
    val allCuboidLayouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    val base: String = KapConfig.getInstanceFromEnv.getReadParquetStoragePath(df.getProject)
    import scala.collection.JavaConversions._
    for (nCuboidLayout <- allCuboidLayouts) {
      val path: String = TableScanPlan.toLayoutPath(df, nCuboidLayout.getId, base, latestReadySegment)
      val schema: StructType = sparkSession.read.parquet(path).schema
      val schemaFromNCuboidLayout: StructType = LayoutEntityConverter.genCuboidSchemaFromNCuboidLayout(nCuboidLayout)
      if (!(schema == StructType.removeMetadata("__CHAR_VARCHAR_TYPE_STRING",schemaFromNCuboidLayout))) {
        throw new RuntimeException(s"Check schema failed : dfName: $dfName, layoutId: ${nCuboidLayout.getId}, actual: ${schemaFromNCuboidLayout.treeString}, expect: ${schema.treeString}")
      }
    }
  }

  def factTableSchemaNameToColumnId(schemaName: String): Int = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    data.apply(data.length - 1).toInt
  }

  def parseDeriveTableSchemaName(schemaName: String): DeriveTableColumnInfo = {
    val data = schemaName.split(SparderConstants.COLUMN_NAME_SEPARATOR)
    try {
      DeriveTableColumnInfo(data.apply(2), data.apply(3).toInt, data.apply(1))
    } catch {
      case e: Exception =>
        throw e
    }
  }

  def generateDeriveTableSchemaName(deriveTableName: String,
                                    colId: Int,
                                    columnName: String = "N"): String = {
    DeriveTableColumnInfo(deriveTableName, colId, columnName).toString
  }

  def replaceToAggravateSchemaName(index: Int,
                                   aggFuncName: String,
                                   hash: String,
                                   aggArgs: String*): String = {
    AggColumnInfo(index, aggFuncName, hash, aggArgs: _*).toString
  }

  def buildFactTableSortNames(sourceSchema: StructType): Array[String] = {
    sourceSchema.fieldNames
      .filter(name => name.startsWith("F" + COLUMN_NAME_SEPARATOR) || name.startsWith("R" + COLUMN_NAME_SEPARATOR))
      .map(name => (factTableSchemaNameToColumnId(name), name))
      .sortBy(_._1)
      .map(_._2)
  }

  def buildSchemaWithRawTable(columnDescs: Array[ColumnDesc]): StructType = {

    StructType(columnDescs.map { columnDesc =>
      StructField(
        columnDesc.getName,
        SparderTypeUtil.toSparkType(columnDesc.getType))
    })
  }

  def genTopNSchema(advanceTableName: String,
                     colId: Int,
                     columnName: String = "N"): String = {
    TopNColumnInfo(advanceTableName, colId, columnName).toString
  }

  def createStructType(name: String,
                       dataType: DataType,
                       nullable: Boolean): StructField =
    StructField(name, dataType, nullable)
}

sealed abstract class ColumnInfo(tableName: String,
                                 columnId: Int,
                                 columnName: String) {
  val prefix: String

  override def toString: String =
    s"$prefix${SparderConstants.COLUMN_NAME_SEPARATOR}$columnName${SparderConstants.COLUMN_NAME_SEPARATOR}$tableName${SparderConstants.COLUMN_NAME_SEPARATOR}$columnId"
}

case class FactTableCulumnInfo(tableName: String,
                               columnId: Int,
                               columnName: String)
    extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "F"
}

case class DeriveTableColumnInfo(tableName: String,
                                 columnId: Int,
                                 columnName: String)
    extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "D"
}

case class AggColumnInfo(index: Int,
                         funcName: String,
                         hash: String,
                         args: String*) {
  override def toString: String =
    s"$funcName(${args.mkString("_")})_${index}_$hash"
}

case class TopNColumnInfo(tableName: String, columnId: Int, columnName: String)
  extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "A"
}
