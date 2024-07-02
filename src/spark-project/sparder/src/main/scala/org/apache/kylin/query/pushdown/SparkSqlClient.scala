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

package org.apache.kylin.query.pushdown

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit, TimeoutException}
import java.util.{UUID, List => JList}

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.cache.kylin.KylinCacheFileSystem
import org.apache.kylin.common.util.{DateFormat, HadoopUtil, Pair}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.fileseg.FileSegments
import org.apache.kylin.engine.spark.QueryCostCollector
import org.apache.kylin.guava30.shaded.common.collect.{ImmutableList, Lists}
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.metadata.query.StructField
import org.apache.kylin.query.mask.QueryResultMasks
import org.apache.kylin.query.runtime.plan.QueryToExecutionIDCache
import org.apache.kylin.query.runtime.plan.ResultPlan.saveAsyncQueryResult
import org.apache.kylin.query.util.{QueryInterruptChecker, SparkJobTrace}
import org.apache.kylin.softaffinity.SoftAffinityManager
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Row, SparderEnv, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}
import scala.concurrent.duration.Duration

object SparkSqlClient {
  val DEFAULT_DB: String = "spark.sql.default.database"

  val SHUFFLE_PARTITION: String = "spark.sql.shuffle.partitions"

  val logger: Logger = LoggerFactory.getLogger(classOf[SparkSqlClient])

  @deprecated
  def executeSql(ss: SparkSession, sql: String, uuid: UUID, project: String): Pair[JList[JList[String]], JList[StructField]] = {
    val results = executeSqlToIterable(ss, sql, uuid, project)
    Pair.newPair(ImmutableList.copyOf(results._1), results._3)
  }

  def executeSqlToIterable(ss: SparkSession, sql: String, uuid: UUID, project: String):
  (java.lang.Iterable[JList[String]], Int, JList[StructField]) = {
    ss.sparkContext.setLocalProperty("spark.scheduler.pool", "query_pushdown")
    HadoopUtil.setCurrentConfiguration(ss.sparkContext.hadoopConfiguration)
    val queryId = QueryContext.current().getQueryId
    ss.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    logger.info("Start to run sql with SparkSQL...")

    try {
      // process cache hint like -- select * from xxx /*+ ACCEPT_CACHE_TIME(158176387682000) */
      KylinCacheFileSystem.processAcceptCacheTimeInHintStr(QueryContext.current().getFirstHintStr)

      val db = if (StringUtils.isNotBlank(project)) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getDefaultDatabase(project)
      } else {
        null
      }
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, db)
      val dfOfPushDown = ss.sql(sql)
      if (NProjectManager.getProjectConfig(project).isPrintQueryPlanEnabled) {
        logger.info(dfOfPushDown.queryExecution.logical.toString())
      }
      val df = QueryResultMasks.maskResult(dfOfPushDown)
      logger.info("SparkSQL returned result DataFrame")
      QueryContext.current().record("to_spark_plan")

      autoSetShufflePartitions(df)
      QueryContext.current().record("auto_set_parts")

      val iter = if (FileSegments.isSyncFileSegSql(sql)) {
        val nParts = df.rdd.getNumPartitions // trigger file scan, whose result will be captured by FileSegmentsDetector
        (
          ImmutableList.of(ImmutableList.of(nParts.toString).asInstanceOf[JList[String]]),
          1,
          ImmutableList.of(new StructField("CNT", -5, "BIGINT", 0, 0, false))
        )
      } else {
        dfToList(ss, sql, df)
      }
      QueryContext.current().record("collect_result")
      SoftAffinityManager.logAuditAsks()
      iter
    } finally {
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, null)
      KylinCacheFileSystem.clearAcceptCacheTimeLocally()
    }
  }

  private def autoSetShufflePartitions(df: DataFrame): Unit = {
    val config = KylinConfig.getInstanceFromEnv
    val oriShufflePartition = df.sparkSession.sessionState.conf.getConfString(SHUFFLE_PARTITION).toInt
    val isSkip = !config.isAutoSetPushDownPartitions || !ResourceDetectUtils.checkPartitionFilter(df.queryExecution.sparkPlan)
    if (isSkip) {
      logger.info(s"Skip auto set $SHUFFLE_PARTITION, use origin value $oriShufflePartition")
      return
    }

    val forced = config.autoSetPushDownPartitionsForced
    if (forced > 0) {
      df.sparkSession.sessionState.conf.setLocalProperty(SHUFFLE_PARTITION, forced.toString)
      QueryContext.current().setShufflePartitions(forced)
      logger.info(s"Auto force set forced spark.sql.shuffle.partitions $forced")
      return
    }

    val isConcurrency = config.isConcurrencyFetchDataSourceSize
    val executor = Executors.newSingleThreadExecutor()
    val timeOut = config.getAutoShufflePartitionTimeOut
    val future = executor.submit(new Callable[Unit] {
      override def call(): Unit = {
        val basePartitionSize = config.getBaseShufflePartitionSize
        val paths = ResourceDetectUtils.getPaths(df.queryExecution.sparkPlan)
        var sourceTableSize: String = ""
        if (isConcurrency) {
          sourceTableSize = ResourceDetectUtils.getResourceSizeWithTimeoutByConcurrency(config,
            Duration(timeOut, TimeUnit.SECONDS), SparderEnv.getHadoopConfiguration(), paths: _*) + "b"
        } else {
          sourceTableSize = ResourceDetectUtils.getResourceSizBySerial(config, SparderEnv.getHadoopConfiguration(),
            paths: _*) + "b"
        }
        val partitions = Math.max(1, JavaUtils.byteStringAsMb(sourceTableSize) / basePartitionSize).toString
        df.sparkSession.sessionState.conf.setLocalProperty(SHUFFLE_PARTITION, partitions)
        QueryContext.current().setShufflePartitions(partitions.toInt)
        logger.info(s"Auto set $SHUFFLE_PARTITION $partitions, " +
          s"sourceTableSize $sourceTableSize, basePartitionSize $basePartitionSize")
      }
    })
    try {
      future.get(timeOut, TimeUnit.SECONDS)
    } catch {
      case e: TimeoutException =>
        val oriShufflePartition = df.sparkSession.sessionState.conf.getConfString(SHUFFLE_PARTITION).toInt
        val partitions = oriShufflePartition * config.getAutoShufflePartitionMultiple
        df.sparkSession.sessionState.conf.setLocalProperty(SHUFFLE_PARTITION, partitions.toString)
        QueryContext.current().setShufflePartitions(partitions)
        logger.info(s"Auto set shuffle partitions timeout. set $SHUFFLE_PARTITION $partitions.")
      case e: Exception =>
        logger.error(s"Auto set $SHUFFLE_PARTITION failed.", e)
        throw e
    } finally {
      executor.shutdownNow()
    }
  }

  /* VisibleForTesting */
  def dfToList(ss: SparkSession, sql: String, df: DataFrame): (java.lang.Iterable[JList[String]], Int, JList[StructField]) = {
    val config = KapConfig.getInstanceFromEnv
    val jobGroup = Thread.currentThread.getName
    ss.sparkContext.setJobGroup(jobGroup, s"Push down: $sql", interruptOnCancel = true)
    try {
      val queryTagInfo = QueryContext.current().getQueryTagInfo
      if (queryTagInfo.isAsyncQuery) {
        val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
        val columnNames = fieldList.asScala.map(field => field.getName).asJava
        QueryContext.current().setColumnNames(columnNames)
        saveAsyncQueryResult(df, queryTagInfo.getFileFormat, queryTagInfo.getFileEncode, null)
        return (Lists.newArrayList(), 0, fieldList)
      } else if (QueryContext.current().isExplainSql) {
        val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
        QueryContext.current().getQueryPlan.setSparkPlan(df.queryExecution.analyzed.toString())
        return (Lists.newArrayList(), 0, fieldList)
      }
      QueryContext.currentTrace().endLastSpan()
      val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace()
        , QueryContext.current().getQueryId, SparderEnv.getSparkSession.sparkContext)

      NProjectManager.getProjectConfig(QueryContext.current().getProject).isQueryUseIterableCollectApi

      val results = if (NProjectManager.getProjectConfig(QueryContext.current().getProject)
        .isQueryUseIterableCollectApi) {
        df.collectToIterator()
      } else {
        df.toIterator()
      }
      val resultRows = results._1
      val resultSize = results._2
      if (config.isQuerySparkJobTraceEnabled) jobTrace.jobFinished()
      val fieldList = df.schema.map(field => SparderTypeUtil.convertSparkFieldToJavaField(field)).asJava
      val (scanRows, scanBytes) = QueryMetricUtils.collectScanMetrics(df.queryExecution.executedPlan)
      val (jobCount, stageCount, taskCount) = QueryMetricUtils.collectTaskRelatedMetrics(jobGroup, ss.sparkContext)
      QueryContext.current().getMetrics.setScanRows(scanRows)
      QueryContext.current().getMetrics.setScanBytes(scanBytes)
      QueryContext.current().getMetrics.setQueryJobCount(jobCount)
      QueryContext.current().getMetrics.setQueryStageCount(stageCount)
      QueryContext.current().getMetrics.setQueryTaskCount(taskCount)
      QueryContext.current().getMetrics.setCpuTime(QueryCostCollector.getAndCleanStatus(QueryContext.current().getQueryId))
      // return result
      (readPushDownResultRow(resultRows, checkInterrupt = true), resultSize, fieldList)
    } catch {
      case e: Throwable =>
        if (e.isInstanceOf[InterruptedException]) {
          Thread.currentThread.interrupt()
          ss.sparkContext.cancelJobGroup(jobGroup)
          QueryInterruptChecker.checkThreadInterrupted("Interrupted at the stage of collecting result in SparkSqlClient.",
            "Current step: Collecting dataset of push-down.")
        }
        throw e
    } finally {
      QueryContext.current().setExecutionID(QueryToExecutionIDCache.getQueryExecutionID(QueryContext.current().getQueryId))
      df.sparkSession.sessionState.conf.setLocalProperty(SHUFFLE_PARTITION, null)
      HadoopUtil.setCurrentConfiguration(null)
    }
  }

  def readPushDownResultRow(resultRows: util.Iterator[Row], checkInterrupt: Boolean): java.lang.Iterable[JList[String]] = {
    () =>
      new java.util.Iterator[JList[String]] {
        /*
         * After fetching a batch of 1000, checks whether the query thread is interrupted.
         */
        val checkInterruptSize = 1000;
        var readRowSize = 0;

        override def hasNext: Boolean = resultRows.hasNext

        override def next(): JList[String] = {
          val row = resultRows.next()
          readRowSize += 1;
          if (checkInterrupt && readRowSize % checkInterruptSize == 0) {
            QueryInterruptChecker.checkThreadInterrupted("Interrupted at the stage of collecting result in SparkSqlClient.",
              "Current step: Collecting dataset of push-down.")
          }
          row.toSeq.map(rawValueToString(_)).asJava
        }
      }
  }

  private def rawValueToString(value: Any, wrapped: Boolean = false): String = value match {
    case null => null
    case value: Timestamp => DateFormat.castTimestampToString(value.getTime)
    case value: String => if (wrapped) "\"" + value + "\"" else value
    case value: mutable.WrappedArray[AnyVal] => value.array.map(v => rawValueToString(v, true)).mkString("[", ",", "]")
    case value: mutable.WrappedArray.ofRef[AnyRef] => value.array.map(v => rawValueToString(v, true)).mkString("[", ",", "]")
    case value: immutable.Map[Any, Any] =>
      value.map(p => rawValueToString(p._1, true) + ":" + rawValueToString(p._2, true)).mkString("{", ",", "}")
    case value: Array[Byte] => new String(value, StandardCharsets.UTF_8)
    case value: BigDecimal => SparderTypeUtil.adjustDecimal(value)
    case value: Any => value.toString
  }
}

class SparkSqlClient
