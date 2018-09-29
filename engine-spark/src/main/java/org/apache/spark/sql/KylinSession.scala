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

import java.io.File
import java.nio.file.Paths
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.sql.manager.UdfManager
import org.apache.spark.util.{KylinReflectUtils, XmlUtils}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

class KylinSession(
                    @transient val sc: SparkContext,
                    @transient private val existingSharedState: Option[SharedState])
  extends SparkSession(sc) {

  def this(sc: SparkContext) {
    this(sc, None)
  }

//  @transient
//  override lazy val sessionState: SessionState =
//    KylinReflectUtils.getSessionState(sc, this).asInstanceOf[SessionState]

  override def newSession(): SparkSession = {
    new KylinSession(sparkContext, Some(sharedState))
  }
}

object KylinSession extends Logging {

  implicit class KylinBuilder(builder: Builder) {
    def getOrCreateKylinSession(): SparkSession = synchronized {
      val options =
        getValue("options", builder)
          .asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder)
          .asInstanceOf[Option[SparkContext]]
      var session: SparkSession = SparkSession.getActiveSession match {
        case Some(sparkSession: KylinSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach {
              case (k, v) => sparkSession.sessionState.conf.setConfString(k, v)
            }
            sparkSession
          } else {
            null
          }
        case _ => null
      }
      if (session ne null) {
        return session
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
          val sparkConf = initSparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          sc
        }
        session = new KylinSession(sparkContext)
        SparkSession.setDefaultSession(session)
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
            SparkSession.sqlListener.set(null)
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

    private lazy val kylinConfig: KylinConfig = KylinConfig.getInstanceFromEnv

    def initSparkConf(): SparkConf = {
      val sparkConf = new SparkConf()

      kylinConfig.getSparkConf.asScala.foreach {
        case (k, v) =>
          sparkConf.set(k, v)
      }
      if (KylinConfig.getInstanceFromEnv.isParquetSeparateFsEnabled) {
        logInfo("ParquetSeparateFs is enabled : begin override read cluster conf to sparkConf")
        addReadConfToSparkConf(sparkConf)
      }
      val instances = sparkConf.get("spark.executor.instances").toInt
      val cores = sparkConf.get("spark.executor.cores").toInt
      val sparkCores = instances * cores
      if (sparkConf.get("spark.sql.shuffle.partitions", "").isEmpty) {
        sparkConf.set("spark.sql.shuffle.partitions", sparkCores.toString)
      }
      sparkConf.set("spark.sql.session.timeZone", "UTC")
      sparkConf.set("spark.debug.maxToStringFields", "1000")
      sparkConf.set("spark.scheduler.mode", "FAIR")
      if (new File(KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml")
        .exists()) {
        val fairScheduler = KylinConfig.getKylinConfDir.getCanonicalPath + "/fairscheduler.xml"
        sparkConf.set("spark.scheduler.allocation.file", fairScheduler)
      }

      if (!"true".equalsIgnoreCase(System.getProperty("spark.local"))) {
        if("yarn-client".equalsIgnoreCase(sparkConf.get("spark.master"))){
          sparkConf.set("spark.yarn.dist.jars", kylinConfig.sparderJars)
        } else {
          sparkConf.set("spark.jars", kylinConfig.sparderJars)
        }

        val filePath = KylinConfig.getInstanceFromEnv.sparderJars
          .split(",")
          .filter(p => p.contains("storage-parquet"))
          .apply(0)
        val fileName = filePath.substring(filePath.lastIndexOf('/') + 1)
        sparkConf.set("spark.executor.extraClassPath", fileName)
      }

      sparkConf
    }


    /**
      * For R/W Splitting
      *
      * @param sparkConf
      * @return
      */
    def addReadConfToSparkConf(sparkConf: SparkConf): SparkConf = {
      val readHadoopConfDir = kylinConfig
        .getColumnarSparkEnv("HADOOP_CONF_DIR")
      if (!new File(readHadoopConfDir).exists()) {
        throw new IllegalArgumentException(
          "kylin.storage.columnar.spark-env.HADOOP_CONF_DIR not found: " + readHadoopConfDir)
      }
      overrideHadoopConfigToSparkConf(readHadoopConfDir, sparkConf)
      changeStagingDir(sparkConf, readHadoopConfDir)
      sparkConf
    }

    private def changeStagingDir(sparkConf: SparkConf, readHadoopConf: String) = {
      val coreProperties: Properties =
        XmlUtils.loadProp(readHadoopConf + "/core-site.xml")
      val path = new Path(coreProperties.getProperty("fs.defaultFS"))
      val homePath =
        path.getFileSystem(new Configuration()).getHomeDirectory.toString
      sparkConf.set("spark.yarn.stagingDir", homePath)
    }

    private def overrideHadoopConfigToSparkConf(
                                                 readHadoopConf: String,
                                                 sparkConf: SparkConf): Unit = {
      val overrideFiles =
        kylinConfig.getParquetSeparateOverrideFiles.split(",")
      overrideFiles.foreach { configFile =>
        logInfo("find config file : " + configFile)
        val cleanedPath = Paths.get(readHadoopConf, configFile)
        val properties: Properties =
          XmlUtils.loadProp(cleanedPath.toString)
        properties
          .entrySet()
          .asScala
          .filter(_.getValue.asInstanceOf[String].nonEmpty)
          .foreach { entry =>
            logInfo("override : " + entry)
            sparkConf.set(
              "spark.hadoop." + entry.getKey.asInstanceOf[String],
              entry.getValue.asInstanceOf[String])
          }
      }
    }
  }

}
