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

import java.sql.Timestamp
import java.util
import java.util.{UUID, List => JList}

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.util.{DateFormat, HadoopUtil, Pair}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.metadata.query.StructField
import org.apache.kylin.query.mask.QueryResultMasks
import org.apache.kylin.query.runtime.plan.QueryToExecutionIDCache
import org.apache.kylin.query.runtime.plan.ResultPlan.saveAsyncQueryResult
import org.apache.kylin.query.util.{QueryInterruptChecker, SparkJobTrace}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.hive.QueryMetricUtils
import org.apache.spark.sql.hive.utils.ResourceDetectUtils
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Row, SparderEnv, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

import com.google.common.collect.ImmutableList

import io.kyligence.kap.guava20.shaded.common.collect.Lists

object SparkSqlClient {
  val DEFAULT_DB: String = "spark.sql.default.database"

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
    val s = "Start to run sql with SparkSQL..."
    val queryId = QueryContext.current().getQueryId
    ss.sparkContext.setLocalProperty(QueryToExecutionIDCache.KYLIN_QUERY_ID_KEY, queryId)
    logger.info(s)

    try {
      val db = if (StringUtils.isNotBlank(project)) {
        NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getDefaultDatabase(project)
      } else {
        null
      }
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, db)
      val df = QueryResultMasks.maskResult(ss.sql(sql))

      autoSetShufflePartitions(df)

      val msg = "SparkSQL returned result DataFrame"
      logger.info(msg)

      dfToList(ss, sql, df)
    } finally {
      ss.sessionState.conf.setLocalProperty(DEFAULT_DB, null)
    }
  }

  private def autoSetShufflePartitions(df: DataFrame) = {
    val config = KylinConfig.getInstanceFromEnv
    if (config.isAutoSetPushDownPartitions) {
      try {
        val basePartitionSize = config.getBaseShufflePartitionSize
        val paths = ResourceDetectUtils.getPaths(df.queryExecution.sparkPlan)
        val sourceTableSize = ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),
          config.isConcurrencyFetchDataSourceSize, paths: _*) + "b"
        val partitions = Math.max(1, JavaUtils.byteStringAsMb(sourceTableSize) / basePartitionSize).toString
        df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", partitions)
        QueryContext.current().setShufflePartitions(partitions.toInt)
        logger.info(s"Auto set spark.sql.shuffle.partitions $partitions, " +
          s"sourceTableSize $sourceTableSize, basePartitionSize $basePartitionSize")
      } catch {
        case e: Throwable =>
          logger.error("Auto set spark.sql.shuffle.partitions failed.", e)
      }
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
      }
      QueryContext.currentTrace().endLastSpan()
      val jobTrace = new SparkJobTrace(jobGroup, QueryContext.currentTrace()
        , QueryContext.current().getQueryId, SparderEnv.getSparkSession.sparkContext)
      val results = df.toIterator()
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
      // return result
      (readPushDownResultRow(resultRows, true), resultSize, fieldList)
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
      df.sparkSession.sessionState.conf.setLocalProperty("spark.sql.shuffle.partitions", null)
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
    case value: Any => value.toString
  }
}

class SparkSqlClient
