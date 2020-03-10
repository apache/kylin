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

import org.apache.spark.internal.Logging
import java.lang.{Boolean => JBoolean}

object KylinSparkEnv extends Logging {
	@volatile
	private var spark: SparkSession = _

	val _cuboid = new ThreadLocal[Dataset[Row]]
	val _needCompute = new ThreadLocal[JBoolean] {
		override protected def initialValue = false
	}

	@volatile
	private var initializingThread: Thread = null

	def getSparkSession: SparkSession = withClassLoad {
		if (spark == null || spark.sparkContext.isStopped) {
			logInfo("Init spark.")
			initSpark()
		}
		spark
	}

	def setSparkSession(sparkSession: SparkSession): Unit = {
		spark = sparkSession
	}

	def init(): Unit = withClassLoad {
		getSparkSession
	}

	def withClassLoad[T](body: => T): T = {
		//    val originClassLoad = Thread.currentThread().getContextClassLoader
		// fixme aron
		//        Thread.currentThread().setContextClassLoader(ClassLoaderUtils.getSparkClassLoader)
		val t = body
		//    Thread.currentThread().setContextClassLoader(originClassLoad)
		t
	}

	def initSpark(): Unit = withClassLoad {
		this.synchronized {
			if (initializingThread == null && (spark == null || spark.sparkContext.isStopped)) {
				initializingThread = new Thread(new Runnable {
					override def run(): Unit = {
						try {
							val sparkSession = System.getProperty("spark.local") match {
								case "true" =>
									SparkSession.builder
											.master("local")
											.appName("sparder-test-sql-context")
											.enableHiveSupport()
											.getOrCreate()
								case _ =>
									SparkSession.builder
											.appName("sparder-sql-context")
											.master("yarn-client")
											//if user defined other master in kylin.properties,
											// it will get overwrite later in org.apache.spark.sql.KylinSession.KylinBuilder.initSparkConf
											.enableHiveSupport()
											.getOrCreate()
							}
							spark = sparkSession
							logInfo("Spark context started successfully with stack trace:")
							logInfo(Thread.currentThread().getStackTrace.mkString("\n"))
							logInfo(
								"Class loader: " + Thread
										.currentThread()
										.getContextClassLoader
										.toString)
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
		}
	}

	def getCuboid: Dataset[Row] = _cuboid.get

	def setCuboid(cuboid: Dataset[Row]): Unit = {
		_cuboid.set(cuboid)
	}

	def skipCompute(): Unit = {
		_needCompute.set(true)
	}
}
