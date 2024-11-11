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
package org.apache.spark.sql.common

import java.io.File

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

object GlutenTestConfig extends Logging {

  private val GLUTEN_CH_LIB_PATH_KEY = "clickhouse.lib.path"

  def configGluten(conf: SparkConf): Unit = {
    val chLibPath = System.getProperty(GLUTEN_CH_LIB_PATH_KEY)
    if (StringUtils.isEmpty(chLibPath) || !new File(chLibPath).exists) {
      log.warn("-Dclickhouse.lib.path is not set or path not exists, skip gluten config")
      return // skip
    }
    conf.set("spark.gluten.enabled", "true")
    conf.set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    conf.set("spark.gluten.sql.columnar.libpath", chLibPath)
    conf.set(
      "spark.gluten.sql.columnar.extended.columnar.pre.rules",
      "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule")
    conf.set(
      "spark.gluten.sql.columnar.extended.expressions.transformer",
      "org.apache.spark.sql.catalyst.expressions.gluten.CustomerExpressionTransformer")

    conf.set("spark.sql.columnVector.offheap.enabled", "true")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "2g")
    conf.set("spark.gluten.sql.enable.native.validation", "false")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    conf.set("spark.gluten.sql.columnar.iterator", "true")
    conf.set("spark.gluten.sql.columnar.sort", "true")
    conf.set("spark.sql.exchange.reuse", "true")
    conf.set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
    conf.set("spark.locality.wait", "0")
    conf.set("spark.locality.wait.node", "0")
    conf.set("spark.locality.wait.process", "0")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB")
    conf.set("spark.gluten.sql.columnar.columnartorow", "true")
    conf.set("spark.gluten.sql.columnar.loadnative", "true")
    conf.set("spark.gluten.sql.columnar.loadarrow", "false")
    conf.set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
    conf.set("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")
    conf.set("spark.databricks.delta.maxSnapshotLineageLength", "20")
    conf.set("spark.databricks.delta.snapshotPartitions", "1")
    conf.set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
    conf.set("spark.databricks.delta.stalenessLimit", "3600000")
    conf.set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
    conf.set("spark.gluten.sql.columnar.coalesce.batches", "false")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "error")
    conf.set("spark.io.compression.codec", "LZ4")
    conf.set("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "LZ4")
    conf.set("spark.gluten.sql.columnar.backend.ch.customized.shuffle.codec.enable", "true")
    conf.set("spark.gluten.sql.columnar.backend.ch.customized.buffer.size", "4096")
    conf.set("spark.gluten.sql.columnar.backend.ch.files.per.partition.threshold", "5")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_config.enable_nullable", "true")
    conf.set(
      "spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.metrics_perf_events_enabled", "false")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "ERROR")
    conf.set(
      "spark.gluten.sql.columnar.backend.ch.runtime_config.local_engine.settings.max_bytes_before_external_group_by",
      "5000000000")
    conf.set("spark.gluten.sql.columnar.maxBatchSize", "32768")
    conf.set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_config.use_local_format", "true")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_settings.use_excel_serialization", "true")
  }

}
