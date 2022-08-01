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
package org.apache.spark.sql

import org.apache.kylin.metadata.cube.model.LayoutEntity
import org.apache.kylin.metadata.model.{FunctionDesc, TblColRef}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil

import java.util.Locale
import scala.collection.JavaConverters._


object LayoutEntityConverter {
  implicit class LayoutEntityConverter(layoutEntity: LayoutEntity) {
    def toCatalogTable(): CatalogTable = {
      val partitionColumn = layoutEntity.getPartitionByColumns.asScala.map(_.toString)
      val bucketSp = {
        if (!layoutEntity.getShardByColumns.isEmpty) {
          BucketSpec(layoutEntity.getBucketNum,
            layoutEntity.getShardByColumns.asScala.map(_.toString),
            layoutEntity.getColOrder.asScala.map(_.toString).filter(!partitionColumn.contains(_)))
        } else {
          null
        }
      }
      CatalogTable(identifier = TableIdentifier(s"${layoutEntity.getId}", Option(layoutEntity.getModel.getId)),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = genCuboidSchemaFromNCuboidLayoutWithPartitionColumn(layoutEntity, partitionColumn),
        partitionColumnNames = partitionColumn,
        bucketSpec = Option(bucketSp))
    }

    def toSchema() : StructType = {
      genCuboidSchemaFromNCuboidLayout(layoutEntity)
    }

    def toExactlySchema() : StructType = {
      genCuboidSchemaFromNCuboidLayout(layoutEntity, true)
    }
  }

  def genCuboidSchemaFromNCuboidLayoutWithPartitionColumn(cuboid: LayoutEntity, partitionColumn: Seq[String]): StructType = {
    val dimensions = cuboid.getOrderedDimensions
    StructType(dimensions.asScala.filter(tp => !partitionColumn.contains(tp._1.toString)).map { i =>
      genSparkStructField(i._1.toString, i._2)
    }.toSeq ++
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          StructField(
            i._1.toString,
            generateFunctionReturnDataType(i._2.getFunction),
            nullable = true)
      }.toSeq ++
      partitionColumn.map(pt => (pt, dimensions.get(pt.toInt))).map { i =>
        genSparkStructField(i._1, i._2)
      })
  }

  def genSparkStructField(name: String, tbl: TblColRef): StructField = {
    val dataTpName = tbl.getType.getName
    if (dataTpName.startsWith("varchar") || dataTpName.startsWith("char")) {
      val meta = new MetadataBuilder().putString("__CHAR_VARCHAR_TYPE_STRING", tbl.getType.toString).build()
      StructField(
        name,
        SparderTypeUtil.toSparkType(tbl.getType),
        nullable = true,
        metadata = meta
      )
    } else {
      StructField(
        name,
        SparderTypeUtil.toSparkType(tbl.getType),
        nullable = true
      )
    }
  }

  def genCuboidSchemaFromNCuboidLayout(cuboid: LayoutEntity, isFastBitmapEnabled: Boolean = false): StructType = {
    val measures = if (isFastBitmapEnabled) {
      val countDistinctColumns = cuboid.listBitmapMeasure().asScala
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          if (countDistinctColumns.contains(i._1.toString)) {
            StructField(i._1.toString, LongType, nullable = true)
          } else {
            StructField(
              i._1.toString,
              generateFunctionReturnDataType(i._2.getFunction),
              nullable = true)
          }
      }.toSeq
    } else {
      cuboid.getOrderedMeasures.asScala.map {
        i =>
          StructField(
            i._1.toString,
            generateFunctionReturnDataType(i._2.getFunction),
            nullable = true)
      }.toSeq
    }

    StructType(cuboid.getOrderedDimensions.asScala.map { i =>
      genSparkStructField(i._1.toString, i._2)
    }.toSeq ++ measures)
  }
  def genBucketSpec(layoutEntity: LayoutEntity, partitionColumn: Set[String]): Option[BucketSpec] = {
    if (layoutEntity.getShardByColumns.isEmpty) {
      Option(BucketSpec(layoutEntity.getBucketNum,
        layoutEntity.getShardByColumns.asScala.map(_.toString),
        layoutEntity.getColOrder.asScala.map(_.toString).filter(!partitionColumn.contains(_))))
    } else {
      Option(null)
    }
  }


  def generateFunctionReturnDataType(function: FunctionDesc): DataType = {
    function.getExpression.toUpperCase(Locale.ROOT) match {
      case "SUM" =>
        val parameter = function.getParameters.get(0)
        if (parameter.isColumnType) {
          SparderTypeUtil.toSparkType(parameter.getColRef.getType, true)
        } else {
          SparderTypeUtil.toSparkType(function.getReturnDataType, true)
        }
      case "COUNT" => LongType
      case x if x.startsWith("TOP_N") =>
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
      case _ => SparderTypeUtil.toSparkType(function.getReturnDataType)
    }
  }
}
