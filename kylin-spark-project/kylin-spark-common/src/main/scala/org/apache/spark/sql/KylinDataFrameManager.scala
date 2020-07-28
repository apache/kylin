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

import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.datasource.FilePruner
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import scala.collection.mutable.{HashMap => MutableHashMap}


class KylinDataFrameManager(sparkSession: SparkSession) {
  private var extraOptions = new MutableHashMap[String, String]()
  private var userSpecifiedSchema: Option[StructType] = None


  /** File format for table */
  def format(source: String): KylinDataFrameManager = {
    option("source", source)
    this
  }

  /** Add key-value to options */
  def option(key: String, value: String): KylinDataFrameManager = {
    this.extraOptions += (key -> value)
    this
  }

  /** Add boolean value to options, for compatibility with Spark */
  def option(key: String, value: Boolean): KylinDataFrameManager = {
    option(key, value.toString)
  }

  /** Add long value to options, for compatibility with Spark */
  def option(key: String, value: Long): KylinDataFrameManager = {
    option(key, value.toString)
  }

  /** Add double value to options, for compatibility with Spark */
  def option(key: String, value: Double): KylinDataFrameManager = {
    option(key, value.toString)
  }

  def isFastBitmapEnabled(isFastBitmapEnabled: Boolean): KylinDataFrameManager = {
    option("isFastBitmapEnabled", isFastBitmapEnabled.toString)
    this
  }

  def cuboidTable(cubeInstance: CubeInstance, layout: Cuboid): DataFrame = {
    option("project", cubeInstance.getProject)
    option("cubeId", cubeInstance.getUuid)
    option("cuboidId", layout.getId)
    val indexCatalog = new FilePruner(cubeInstance, layout, sparkSession, options = extraOptions.toMap)
    sparkSession.baseRelationToDataFrame(
      HadoopFsRelation(
        indexCatalog,
        partitionSchema = indexCatalog.partitionSchema,
        dataSchema = indexCatalog.dataSchema.asNullable,
        bucketSpec = None,
        new ParquetFileFormat,
        options = extraOptions.toMap)(sparkSession))
  }

  /**
   * Specifies the input schema. Some data sources (e.g. JSON) can infer the input schema
   * automatically from data. By specifying the schema here, the underlying data source can
   * skip the schema inference step, and thus speed up data loading.
   *
   * @since 1.4.0
   */
  def schema(schema: StructType): KylinDataFrameManager = {
    this.userSpecifiedSchema = Option(schema)
    this
  }

}
