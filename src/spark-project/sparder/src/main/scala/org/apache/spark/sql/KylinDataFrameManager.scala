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

import java.sql.Timestamp
import org.apache.kylin.metadata.cube.model.{LayoutEntity, NDataflow, NDataflowManager}
import org.apache.kylin.metadata.model.FusionModelManager
import io.kyligence.kap.secondstorage.SecondStorage
import org.apache.kylin.common.KylinConfig
import org.apache.spark.sql.datasource.storage.StorageStoreFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{HashMap => MutableHashMap}

class KylinDataFrameManager(sparkSession: SparkSession) {
  private var extraOptions = new MutableHashMap[String, String]()
  private var userSpecifiedSchema: Option[StructType] = None


  /** File format for table */
  private def format(source: String): KylinDataFrameManager = {
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

  def bucketingEnabled(bucketingEnabled: Boolean): KylinDataFrameManager = {
    option("bucketingEnabled", bucketingEnabled)
  }

  def cuboidTable(dataflow: NDataflow, layout: LayoutEntity, pruningInfo: String): DataFrame = {
    format("parquet")
    option("project", dataflow.getProject)
    option("dataflowId", dataflow.getUuid)
    option("cuboidId", layout.getId)
    option("pruningInfo", pruningInfo)
    if (dataflow.isStreaming && dataflow.getModel.isFusionModel) {
      val fusionModel = FusionModelManager.getInstance(KylinConfig.getInstanceFromEnv, dataflow.getProject)
              .getFusionModel(dataflow.getModel.getFusionId)
      val batchModelId = fusionModel.getBatchModel.getUuid
      val batchDataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, dataflow.getProject).getDataflow(batchModelId)
      val end = batchDataflow.getDateRangeEnd

      val partition = dataflow.getModel.getPartitionDesc.getPartitionDateColumnRef
      val id = layout.getOrderedDimensions.inverse().get(partition)
      SecondStorage.trySecondStorage(sparkSession, dataflow, layout, pruningInfo).getOrElse {
        var df = StorageStoreFactory.create(dataflow.getModel.getStorageType)
          .read(dataflow, layout, sparkSession, extraOptions.toMap)
        if (end != Long.MinValue) {
          df = df.filter(col(id.toString).geq(new Timestamp(end)))
        }
        df
      }
    } else {
      SecondStorage.trySecondStorage(sparkSession, dataflow, layout, pruningInfo).getOrElse {
        StorageStoreFactory.create(dataflow.getModel.getStorageType)
          .read(dataflow, layout, sparkSession, extraOptions.toMap)
      }
    }
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

object KylinDataFrameManager {
  def apply(session: SparkSession): KylinDataFrameManager = {
    new KylinDataFrameManager(session)
  }
}
