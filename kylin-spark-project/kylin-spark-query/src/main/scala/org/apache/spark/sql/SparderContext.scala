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

import java.lang.{Boolean => JBoolean, String => JString}
import java.nio.file.Paths

import org.apache.spark.memory.MonitorEnv
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.kylin.query.UdfManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.KylinSession._
import java.util.concurrent.atomic.AtomicReference

import org.apache.commons.io.FileUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.ToolUtil
import org.apache.kylin.query.monitor.SparderContextCanary
import org.apache.kylin.spark.classloader.ClassLoaderUtils
import org.apache.spark.deploy.StandaloneAppClient
import org.apache.spark.sql.SparderContext.master_app_url
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, ShardFileStatusCache}
import org.apache.spark.sql.metrics.SparderMetricsListener
import org.apache.spark.utils.YarnInfoFetcherUtils

// scalastyle:off
object SparderContext extends Logging {
  @volatile
  private var spark: SparkSession = _

  @volatile
  private var initializingThread: Thread = null

  @volatile
  var master_app_url: String = _

  def getOriginalSparkSession: SparkSession = {
    if (spark == null || spark.sparkContext.isStopped) {
      logInfo("Init spark.")
      initSpark()
    }
    spark
  }

  def getSparkSession: SparkSession = {
    logInfo(s"Current thread ${Thread.currentThread().getId} create a SparkSession.")
    SparderContextFacade.current().getFirst
  }

  def closeThreadSparkSession(): Unit = {
    logInfo(s"Remove SparkSession from thread ${Thread.currentThread().getId}")
    SparderContextFacade.remove()
  }

  def setSparkSession(sparkSession: SparkSession): Unit = {
    spark = sparkSession
    UdfManager.create(sparkSession)
  }

  def setAPPMasterTrackURL(url: String): Unit = {
    master_app_url = url
  }

  def appMasterTrackURL(): String = {
    if (master_app_url != null)
      master_app_url
    else
      ""
  }

  def isSparkAvailable: Boolean = {
    spark != null && !spark.sparkContext.isStopped
  }

  def restartSpark(): Unit = {
    this.synchronized {
      if (spark != null && !spark.sparkContext.isStopped) {
        Utils.tryWithSafeFinally {
          spark.stop()
        } {
          SparkContext.clearActiveContext
        }
      }

      logInfo("Restart Spark")
      init()
    }
  }

  def stopSpark(): Unit = {
    this.synchronized {
      if (spark != null && !spark.sparkContext.isStopped) {
        Utils.tryWithSafeFinally {
          spark.stop()
        } {
          SparkContext.clearActiveContext
        }
      }
    }
  }

  def init(): Unit = {
    getOriginalSparkSession
  }

  def getSparkConf(key: String): String = {
    getSparkSession.sparkContext.conf.get(key)
  }

  def initSpark(): Unit = {
    this.synchronized {
      if (initializingThread == null && (spark == null || spark.sparkContext.isStopped)) {
        initializingThread = new Thread(new Runnable {
          override def run(): Unit = {
            try {
              val kylinConf: KylinConfig = KylinConfig.getInstanceFromEnv
              val master = System.getProperty("spark.local") match {
                case "true" =>
                  "local"
                case _ =>
                  kylinConf.getSparderConfigOverrideWithSpecificName("spark.master")
              }
              logInfo("SparderContext deploy with spark master: " + master)
              val sparkSession = SparkSession.builder
                .master(master)
                .config("spark.submit.deployMode", "client")
                .appName(kylinConf.getSparderAppName)
                .withExtensions { ext =>
                  ext.injectPlannerStrategy(_ => KylinSourceStrategy)
                }
                .enableHiveSupport()
                .getOrCreateKylinSession()

              if (kylinConf.isKylinMetricsReporterForQueryEnabled) {
                val appStatusListener = new SparderMetricsListener()
                sparkSession.sparkContext.addSparkListener(appStatusListener)
                logInfo("Query metrics reporter is enabled, sparder metrics listener is added.")
              }
              spark = sparkSession
              val appid = sparkSession.sparkContext.applicationId
              // write application id to file 'sparkappid'
              val kylinHomePath = KylinConfig.getKylinHomeAtBestEffort().getCanonicalPath
              try {
                val appidFile = Paths.get(kylinHomePath, "sparkappid").toFile
                FileUtils.writeStringToFile(appidFile, appid)
                logInfo("Spark application id is " + appid)
              } catch {
                case e: Exception =>
                  logError("Failed to generate spark application id[" + appid + "] file", e)
              }

              logInfo("Spark context started successfully with stack trace:")
              logInfo(Thread.currentThread().getStackTrace.mkString("\n"))
              logInfo(
                "Class loader: " + Thread
                  .currentThread()
                  .getContextClassLoader
                  .toString)
              initMonitorEnv()
              master match {
                case mode: String if mode.startsWith("local") =>
                  var hostName = ToolUtil.getHostName
                  if (hostName.equals("Unknown")) {
                    hostName = "localhost"
                  }
                  master_app_url = "http://" + hostName + ":" + sparkSession.sparkContext.getConf
                    .get("spark.ui.port", "4040")
                case mode: String if mode.startsWith("spark") =>
                  val hostName = StandaloneAppClient.getMasterHost(kylinConf.getSparderConfigOverrideWithSpecificName("spark.master"))
                  master_app_url = "http://" + hostName + ":" + sparkSession.sparkContext.getConf
                    .get("spark.ui.port", "4040")
                case _ =>
                  master_app_url = YarnInfoFetcherUtils.getTrackingUrl(appid)
              }
            } catch {
              case throwable: Throwable =>
                logError("Error for initializing spark ", throwable)
            } finally {
              logInfo("Setting initializing Spark thread to null.")
              initializingThread = null
            }
          }
        })

        logInfo("Initializing Spark thread starting.")
        initializingThread.start()
      }

      if (initializingThread != null) {
        logInfo("Initializing Spark, waiting for done.")
        initializingThread.join()
      }

      if (System.getProperty("spark.local") != "true" && KylinConfig.getInstanceFromEnv.isSparderCanaryEnabled) {
        //monitor sparder
        SparderContextCanary.init()
      }

      // init FileStatusCache
      ShardFileStatusCache.getFileStatusCache(getOriginalSparkSession)
    }
  }

  def registerListener(sc: SparkContext): Unit = {
    val sparkListener = new SparkListener {

      override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
        case _ => // Ignore
      }
    }
    sc.addSparkListener(sparkListener)
  }

  def initMonitorEnv(): Unit = {
    val env = SparkEnv.get
    val rpcEnv = env.rpcEnv
    val sparkConf = new SparkConf
    MonitorEnv.create(sparkConf, env.executorId, rpcEnv, null, isDriver = true)
    logInfo("setup master endpoint finished." + "hostPort:" + rpcEnv.address.hostPort)
  }

  /**
   * @param sqlText SQL to be validated
   * @return The logical plan
   * @throws ParseException if validate failed
   */
  @throws[ParseException]
  def validateSql(sqlText: String): LogicalPlan = {
    val logicalPlan: LogicalPlan = getSparkSession.sessionState.sqlParser.parsePlan(sqlText)
    logicalPlan
  }

  val _isAsyncQuery = new ThreadLocal[JBoolean]
  val _separator = new ThreadLocal[JString]
  val _df = new ThreadLocal[Dataset[Row]]
  val _needCompute = new ThreadLocal[JBoolean] {
    override protected def initialValue = false
  }

  //cleaned
  val _numScanFiles =
    new ThreadLocal[java.lang.Long] {
      override protected def initialValue = 0L
    }

  val _queryRef =
    new ThreadLocal[AtomicReference[java.lang.Boolean]]

  def accumulateScanFiles(numFiles: java.lang.Long): Unit = {
    _numScanFiles.set(_numScanFiles.get() + numFiles)
  }

  def getNumScanFiles(): java.lang.Long = {
    _numScanFiles.get()
  }

  def setAsAsyncQuery(): Unit = {
    _isAsyncQuery.set(true)
  }

  def isAsyncQuery: java.lang.Boolean =
    if (_isAsyncQuery.get == null) false
    else _isAsyncQuery.get

  def setSeparator(separator: java.lang.String): Unit = {
    _separator.set(separator)
  }

  def getSeparator: java.lang.String =
    if (_separator.get == null) ","
    else _separator.get

  def getDF: Dataset[Row] = _df.get

  def setDF(df: Dataset[Row]): Unit = {
    _df.set(df)
  }

  def setResultRef(ref: AtomicReference[java.lang.Boolean]): Unit = {
    _queryRef.set(ref)
  }

  def getResultRef: AtomicReference[java.lang.Boolean] = _queryRef.get

  // clean it after query end
  def clean(): Unit = {
    _isAsyncQuery.set(null)
    _separator.set(null)
    _df.set(null)
    _needCompute.set(null)
  }

  // clean it after collect
  def cleanQueryInfo(): Unit = {
    _numScanFiles.set(0L)
  }

  def needCompute(): JBoolean = {
    !_needCompute.get()
  }

  def skipCompute(): Unit = {
    _needCompute.set(true)
  }

  def cleanCompute(): Unit = {
    _needCompute.set(false)
  }

}
