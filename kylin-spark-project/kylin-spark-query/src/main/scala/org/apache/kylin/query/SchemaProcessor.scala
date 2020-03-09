/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */
package org.apache.kylin.query

import java.util

import io.kyligence.kap.metadata.cube.gridtable.NCuboidToGridTableMapping
import io.kyligence.kap.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager, NDataSegment}
import io.kyligence.kap.query.runtime.plan.TableScanPlan
import org.apache.kylin.common.util.ImmutableBitSet
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.metadata.model.{ColumnDesc, FunctionDesc}
import org.apache.spark.sql.{LayoutEntityConverter, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{SparderConstants, SparderTypeUtil}
import org.apache.spark.sql.utils.SparkTypeUtil

import scala.collection.JavaConverters._

// scalastyle:off
object SchemaProcessor {

  def buildGTSchema(
    cuboid: LayoutEntity,
    mapping: NCuboidToGridTableMapping,
    tableName: String): Seq[String] = {

    genColumnNames(tableName, cuboid, mapping)
  }

  private def genColumnNames(tableName: String, cuboid: LayoutEntity, mapping: NCuboidToGridTableMapping) = {
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

  def checkSchema(sparkSession: SparkSession, dfName: String, project: String): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(config, project)
    val df: NDataflow = dsMgr.getDataflow(dfName)
    val latestReadySegment: NDataSegment = df.getQueryableSegments.getFirstSegment
    val allCuboidLayouts: util.List[LayoutEntity] = df.getIndexPlan.getAllLayouts
    val base: String = KapConfig.getInstanceFromEnv.getReadParquetStoragePath(df.getProject)
    import scala.collection.JavaConversions._
    for (nCuboidLayout <- allCuboidLayouts) {
      val path: String = TableScanPlan.toCuboidPath(df, nCuboidLayout.getId, base, latestReadySegment)
      val schema: StructType = sparkSession.read.parquet(path).schema
      val schemaFromNCuboidLayout: StructType = LayoutEntityConverter.genCuboidSchemaFromNCuboidLayout(nCuboidLayout)
      if (!(schema == schemaFromNCuboidLayout)) {
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
    s"$prefix${SparderConstants.COLUMN_NAME_SEPARATOR}$columnName${SparderConstants.COLUMN_NAME_SEPARATOR}$tableName${SparderConstants.COLUMN_NAME_SEPARATOR}$columnId"
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
    s"$funcName(${args.mkString("_")})_${index}_$hash"
}

case class TopNColumnInfo(tableName: String, columnId: Int, columnName: String)
  extends ColumnInfo(tableName, columnId, columnName) {
  override val prefix: String = "A"
}