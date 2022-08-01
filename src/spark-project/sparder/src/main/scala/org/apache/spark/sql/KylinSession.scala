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

import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter, PrintWriter}
import java.net.URI
import java.nio.file.Paths
import org.apache.kylin.query.util.ExtractFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.common.util.{HadoopUtil, Unsafe}
import org.apache.kylin.metadata.query.BigQueryThresholdUpdater
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.{SQLConf, SessionState, SharedState, StaticSQLConf}
import org.apache.spark.sql.kylin.external.{KylinSessionStateBuilder, KylinSharedState}
import org.apache.spark.sql.udf.UdfManager
import org.apache.spark.util.{KylinReflectUtils, Utils}
import org.apache.spark.{SparkConf, SparkContext}
import org.springframework.expression.common.TemplateParserContext
import org.springframework.expression.spel.standard.SpelExpressionParser

import java.util
import scala.collection.JavaConverters._

class KylinSession(
                    @transient val sc: SparkContext,
                    @transient private val existingSharedState: Option[SharedState],
                    @transient private val parentSessionState: Option[SessionState],
                    @transient override private[sql] val extensions: SparkSessionExtensions)
  extends SparkSession(sc) {
  def this(sc: SparkContext) {
    this(sc,
      existingSharedState = None,
      parentSessionState = None,
      extensions = null)
  }

  @transient
  override lazy val sharedState: SharedState = {

    val className = KylinConfig.getInstanceFromEnv.getExternalCatalogClass
    val loadExternal = KylinSharedState.checkExternalClass(className)

    if (loadExternal) {
      existingSharedState.getOrElse(new KylinSharedState(sc, className))
    } else {
      // see https://stackoverflow.com/questions/45935672/scala-why-cant-we-do-super-val
      // we can't call  super.sharedState, copy SparkSession#sharedState
      existingSharedState.getOrElse(new SharedState(sparkContext, Map.empty))
    }
  }

  @transient
  override lazy val sessionState: SessionState = {
    val loadExteral = sharedState.isInstanceOf[KylinSharedState]
    if (loadExteral) {
      new KylinSessionStateBuilder(this, parentSessionState).build()
    } else {
      KylinReflectUtils.getSessionState(sc, this, parentSessionState).asInstanceOf[SessionState]
    }
  }

  override def newSession(): KylinSession = {
    val session = new KylinSession(sparkContext, Some(sharedState), parentSessionState = None, extensions)
    SQLConf.setSQLConfGetter(() => session.sessionState.conf)
    session
  }

  override def cloneSession(): SparkSession = {
    val result = new KylinSession(
      sparkContext,
      Some(sharedState),
      Some(sessionState),
      extensions)
    SQLConf.setSQLConfGetter(() => result.sessionState.conf)
    result.sessionState // force copy of SessionState
    result
  }
}

object KylinSession extends Logging {
  def NORMAL_FAIR_SCHEDULER_FILE_NAME: String = "/fairscheduler.xml"
  def QUERY_LIMIT_FAIR_SCHEDULER_FILE_NAME: String = "/query-limit-fair-scheduler.xml"

  implicit class KylinBuilder(builder: Builder) {
    var queryCluster: Boolean = true

    def getOrCreateKylinSession(): SparkSession = synchronized {
      val options =
        getValue("options", builder)
          .asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder)
          .asInstanceOf[Option[SparkContext]]
      val extensions = getValue("extensions", builder)
        .asInstanceOf[SparkSessionExtensions]
      var (session, existingSharedState, parentSessionState) = SparkSession.getActiveSession match {
        case Some(sparkSession: KylinSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach {
              case (k, v) => sparkSession.sessionState.conf.setConfString(k, v)
            }
            (sparkSession, None, None)
          } else if ((sparkSession ne null) && sparkSession.sparkContext.isStopped) {
            (null, Some(sparkSession.sharedState), Some(sparkSession.sessionState))
          } else {
            (null, None, None)
          }
        case _ => (null, None, None)
      }
      if (session ne null) {
        return session
      }

      // for testing only
      // discard existing shardState directly
      // use in case that external shard state needs to be set in UT
      if (Option(System.getProperty("kylin.spark.discard-shard-state")).getOrElse("false").toBoolean) {
        existingSharedState = None
        parentSessionState = None
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession match {
          case Some(sparkSession: KylinSession) =>
            if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
              sparkSession
            } else {
              null
            }
          case _ => null
        }
        if (session ne null) {
          return session
        }
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val conf = new SparkConf()
          options.foreach { case (k, v) => conf.set(k, v) }
          val sparkConf: SparkConf = if (queryCluster) {
            initSparkConf(conf)
          } else {
            conf
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession

          // KE-12678
          if (sc.master.startsWith("yarn")) {
            Unsafe.setProperty("spark.ui.proxyBase", "/proxy/" + sc.applicationId)
          }

          sc
        }
        applyExtensions(
          sparkContext.getConf.get(StaticSQLConf.SPARK_SESSION_EXTENSIONS).getOrElse(Seq.empty),
          extensions)
        session = new KylinSession(sparkContext, existingSharedState, parentSessionState, extensions)
        SQLConf.setSQLConfGetter(() => session.sessionState.conf)
        SparkSession.setDefaultSession(session)
        SparkSession.setActiveSession(session)
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
          }
        })
        UdfManager.create(session)
        session
      }
    }

    def getValue(name: String, builder: Builder): Any = {
      val currentMirror = scala.reflect.runtime.currentMirror
      val instanceMirror = currentMirror.reflect(builder)
      val m = currentMirror
        .classSymbol(builder.getClass)
        .toType
        .members
        .find { p =>
          p.name.toString.equals(name)
        }
        .get
        .asTerm
      instanceMirror.reflectField(m).get
    }

    private lazy val kapConfig: KapConfig = KapConfig.getInstanceFromEnv

    def initSparkConf(sparkConf: SparkConf): SparkConf = {
      if (sparkConf.getBoolean("user.kylin.session", defaultValue = false)) {
        return sparkConf
      }
      sparkConf.set("spark.amIpFilter.enabled", "false")
      if (!KylinConfig.getInstanceFromEnv.getChannel.equalsIgnoreCase("cloud")) {
        sparkConf.set("spark.executor.plugins", "org.apache.spark.memory.MonitorExecutorExtension")
      }
      // kerberos
      if (kapConfig.isKerberosEnabled) {
        sparkConf.set("spark.yarn.keytab", kapConfig.getKerberosKeytabPath)
        sparkConf.set("spark.yarn.principal", kapConfig.getKerberosPrincipal)
        sparkConf.set("spark.yarn.security.credentials.hive.enabled", "false")
      }

      if (UserGroupInformation.isSecurityEnabled) {
        sparkConf.set("hive.metastore.sasl.enabled", "true")
      }

      kapConfig.getSparkConf.asScala.foreach {
        case (k, v) =>
          sparkConf.set(k, v)
      }
      val instances = sparkConf.get("spark.executor.instances").toInt
      val cores = sparkConf.get("spark.executor.cores").toInt
      val sparkCores = instances * cores
      if (sparkConf.get("spark.sql.shuffle.partitions", "").isEmpty) {
        sparkConf.set("spark.sql.shuffle.partitions", sparkCores.toString)
      }

      BigQueryThresholdUpdater.initBigQueryThresholdBySparkResource(instances, cores)

      sparkConf.set("spark.debug.maxToStringFields", "1000")
      sparkConf.set("spark.scheduler.mode", "FAIR")
      val cartesianFactor = KylinConfig.getInstanceFromEnv.getCartesianPartitionNumThresholdFactor
      var cartesianPartitionThreshold = sparkCores * cartesianFactor
      val confThreshold = sparkConf.get("spark.sql.cartesianPartitionNumThreshold")
      if (confThreshold.nonEmpty && confThreshold.toInt >= 0) {
        cartesianPartitionThreshold = confThreshold.toInt
      }
      sparkConf.set("spark.sql.cartesianPartitionNumThreshold", cartesianPartitionThreshold.toString)

      val fairSchedulerConfigDirPath = KylinConfig.getKylinConfDir.getCanonicalPath
      applyFairSchedulerConfig(kapConfig, fairSchedulerConfigDirPath, sparkConf)

      if (kapConfig.isQueryEscapedLiteral) {
        sparkConf.set("spark.sql.parser.escapedStringLiterals", "true")
      }

      if (!"true".equalsIgnoreCase(System.getProperty("spark.local"))) {
        if (sparkConf.get("spark.master").startsWith("yarn")) {
          // TODO Less elegant implementation.
          val applicationJar = KylinConfig.getInstanceFromEnv.getKylinJobJarPath
          val yarnDistJarsConf = "spark.yarn.dist.jars"
          val distJars = if (sparkConf.contains(yarnDistJarsConf)) {
            s"${sparkConf.get(yarnDistJarsConf)},$applicationJar"
          } else {
            applicationJar
          }
          sparkConf.set(yarnDistJarsConf, distJars)
          sparkConf.set("spark.yarn.dist.files", kapConfig.sparderFiles())
        } else {
          sparkConf.set("spark.jars", kapConfig.sparderJars)
          sparkConf.set("spark.files", kapConfig.sparderFiles())
        }

        val fileName = KylinConfig.getInstanceFromEnv.getKylinJobJarPath
        sparkConf.set("spark.executor.extraClassPath", Paths.get(fileName).getFileName.toString)

        val krb5conf = " -Djava.security.krb5.conf=./__spark_conf__/__hadoop_conf__/krb5.conf"
        val executorExtraJavaOptions =
          sparkConf.get("spark.executor.extraJavaOptions", "")
        var executorKerberosConf = ""
        if (kapConfig.isKerberosEnabled && (kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)
          || kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.TDH_PLATFORM))) {
          executorKerberosConf = krb5conf
        }
        sparkConf.set("spark.executor.extraJavaOptions",
          s"$executorExtraJavaOptions -Duser.timezone=${kapConfig.getKylinConfig.getTimeZone} $executorKerberosConf")

        val yarnAMJavaOptions =
          sparkConf.get("spark.yarn.am.extraJavaOptions", "")
        var amKerberosConf = ""
        if (kapConfig.isKerberosEnabled && (kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.FI_PLATFORM)
          || kapConfig.getKerberosPlatform.equalsIgnoreCase(KapConfig.TDH_PLATFORM))) {
          amKerberosConf = krb5conf
        }
        sparkConf.set("spark.yarn.am.extraJavaOptions",
          s"$yarnAMJavaOptions $amKerberosConf")
      }

      if (KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect > 0L) {
        sparkConf.set("spark.sql.driver.maxMemoryUsageDuringCollect", KylinConfig.getInstanceFromEnv.getQueryMemoryLimitDuringCollect + "m")
      }

      val eventLogEnabled = sparkConf.getBoolean("spark.eventLog.enabled", defaultValue = false)
      var logDir = sparkConf.get("spark.eventLog.dir", "")
      if (eventLogEnabled && logDir.nonEmpty) {
        logDir = ExtractFactory.create.getSparderEvenLogDir()
        sparkConf.set("spark.eventLog.dir", logDir)
        val logPath = new Path(new URI(logDir).getPath)
        val fs = HadoopUtil.getWorkingFileSystem()
        if (!fs.exists(logPath)) {
          fs.mkdirs(logPath)
        }
      }

      if (kapConfig.getKylinConfig.asyncProfilingEnabled()) {
        val plugins = sparkConf.get("spark.plugins", "")
        if (plugins.isEmpty) {
          sparkConf.set("spark.plugins", "org.apache.kylin.query.asyncprofiler.AsyncProfilerSparkPlugin")
        } else {
          sparkConf.set("spark.plugins", "org.apache.kylin.query.asyncprofiler.AsyncProfilerSparkPlugin," + plugins)
        }
      }

      sparkConf
    }

    def buildCluster(): KylinBuilder = {
      queryCluster = false
      this
    }
  }

  /**
    * Copied from SparkSession.applyExtensions. So that KylinSession can load extensions through SparkConf.
    * <p/>
    * Initialize extensions for given extension classnames. The classes will be applied to the
    * extensions passed into this function.
    */
  private def applyExtensions(
                               extensionConfClassNames: Seq[String],
                               extensions: SparkSessionExtensions): SparkSessionExtensions = {
    extensionConfClassNames.foreach { extensionConfClassName =>
      try {
        val extensionConfClass = Utils.classForName(extensionConfClassName)
        val extensionConf = extensionConfClass.getConstructor().newInstance()
          .asInstanceOf[SparkSessionExtensions => Unit]
        extensionConf(extensions)
      } catch {
        // Ignore the error if we cannot find the class or when the class has the wrong type.
        case e@(_: ClassCastException |
                _: ClassNotFoundException |
                _: NoClassDefFoundError) =>
          logWarning(s"Cannot use $extensionConfClassName to configure session extensions.", e)
      }
    }
    extensions
  }

  def applyFairSchedulerConfig(kapConfig: KapConfig, confFileDirPath: String, sparkConf: SparkConf): Unit = {
    var fairScheduler: String = null
    val isQueryLimitValid = kapConfig.isQueryLimitEnabled && SparderEnv.isSparkExecutorResourceLimited(sparkConf)
    if (isQueryLimitValid) {
      var executorNum = sparkConf.get("spark.executor.instances").toInt
      val dynamicAllocationEnabled = sparkConf.get("spark.dynamicAllocation.enabled", "false").toBoolean
      if (dynamicAllocationEnabled) {
        executorNum = sparkConf.get("spark.dynamicAllocation.maxExecutors").toInt
      }
      val cores = sparkConf.get("spark.executor.cores").toInt
      fairScheduler = confFileDirPath + KylinSession.QUERY_LIMIT_FAIR_SCHEDULER_FILE_NAME
      prepareQueryLimitSchedulerConfig(executorNum * cores, fairScheduler)
      fairScheduler = "file://" + fairScheduler
      sparkConf.set("spark.scheduler.allocation.file", fairScheduler)
    } else if (new File(confFileDirPath + KylinSession.NORMAL_FAIR_SCHEDULER_FILE_NAME).exists()) {
      fairScheduler = "file://" + confFileDirPath + KylinSession.NORMAL_FAIR_SCHEDULER_FILE_NAME
      sparkConf.set("spark.scheduler.allocation.file", fairScheduler)
    }
  }

  def prepareQueryLimitSchedulerConfig(sparkSlots: Int, fairSchedulerConfPath: String): Unit = {
    val params = new java.util.HashMap[String, Int]
    params.put("heavyTaskPoolWeight", sparkSlots)
    params.put("lightweightTaskPoolWeight", Math.pow(sparkSlots, 2).toInt + 1)
    params.put("lightweightTaskPoolMinShare", sparkSlots)
    params.put("vipTaskPoolWeight", Math.pow(sparkSlots, 2).toInt + 2)
    params.put("vipTaskPoolMinShare", Math.pow(sparkSlots, 2).toInt + 1)

    val confTemplateFile = new File(fairSchedulerConfPath + ".template")
    val fileReader = new BufferedReader(new FileReader(confTemplateFile))
    val confFile = new File(fairSchedulerConfPath)
    val fileWriter = new BufferedWriter(new FileWriter(confFile))
    var templateLine: String = null
    var processedLine: String = null
    val parser = new SpelExpressionParser()
    val parserCtx = new TemplateParserContext()
    while ( {
      templateLine = fileReader.readLine(); templateLine != null
    }) {
      processedLine = parser.parseExpression(templateLine, parserCtx).getValue(params, classOf[String]) + "\r\n"
      fileWriter.write(processedLine)
    }
    fileReader.close()
    fileWriter.close()
  }

}
