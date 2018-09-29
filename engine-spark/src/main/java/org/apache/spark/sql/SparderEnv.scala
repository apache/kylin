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

import org.apache.spark.internal.Logging;
import org.apache.spark.sql.KylinSession._
import org.apache.kylin.ext.ClassLoaderUtils;

object SparderEnv extends Logging {
  @volatile
  private var spark: SparkSession = _

  def getSparkSession: SparkSession = withClassLoad {
    if (spark == null || spark.sparkContext.isStopped) {
      logInfo("Init spark.")
      initSpark()
    }
    spark
  }

  def isSparkAvailable: Boolean = {
    spark != null && !spark.sparkContext.isStopped
  }

  def restartSpark(): Unit = withClassLoad {
    this.synchronized {
      if (spark != null && !spark.sparkContext.isStopped) {
        spark.stop()
      }

      logInfo("Restart Spark")
      init()
    }
  }

  def init(): Unit = withClassLoad {
    getSparkSession
  }

  def getSparkConf(key: String): String = {
    getSparkSession.sparkContext.conf.get(key)
  }

  def getActiveJobs(): Int = {
    SparderEnv.getSparkSession.sparkContext.jobProgressListener.activeJobs.size
  }

  def getFailedJobs(): Int = {
    SparderEnv.getSparkSession.sparkContext.jobProgressListener.failedJobs.size
  }

  def getAsyncResultCore: Int = {
    val sparkConf = getSparkSession.sparkContext.getConf
    val instances = sparkConf.get("spark.executor.instances").toInt
    val cores = sparkConf.get("spark.executor.cores").toInt
    Math.round(instances * cores / 3)
  }

  def initSpark(): Unit = withClassLoad {
    this.synchronized {
      if (spark == null || spark.sparkContext.isStopped) {
        val sparkSession = System.getProperty("spark.local") match {
          case "true" =>
            SparkSession.builder
              .master("local")
              .appName("test-local-sql-context")
//                        .enableHiveSupport()
              .getOrCreateKylinSession()
          case _ =>
            SparkSession.builder
              .appName("test-sql-context")
              .enableHiveSupport()
              .getOrCreateKylinSession()
        }
        spark = sparkSession

        logInfo("Spark context started successfully with stack trace:")
        logInfo(Thread.currentThread().getStackTrace.mkString("\n"))
        logInfo("Class loader: " + Thread.currentThread().getContextClassLoader.toString)
      }
    }
  }

  /**
    * To avoid spark being affected by the environment, we use spark classloader load spark.
    *
    * @param body Somewhere if you use spark
    * @tparam T Action function
    * @return The body return
    */
  def withClassLoad[T](body: => T): T = {
    val originClassLoad = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader)
    val t = body
    Thread.currentThread().setContextClassLoader(originClassLoad)
    t
  }
}
