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

package org.apache.spark.sql.manager

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.udf.SparderAggFun

class UdfManager(sparkSession: SparkSession) extends Logging {
  private var udfCache: Cache[String, String] = _

  udfCache = CacheBuilder.newBuilder
    .maximumSize(100)
    .expireAfterWrite(1, TimeUnit.HOURS)
    .removalListener(new RemovalListener[String, String]() {
      override def onRemoval(notification: RemovalNotification[String, String]): Unit = {
        val func = notification.getKey
        logInfo(s"remove function $func")
      }
    })
    .build
    .asInstanceOf[Cache[String, String]]

  def destory(): Unit = {
    udfCache.cleanUp()
  }

  def doRegister(dataType: DataType, funcName: String): String = {
    val name = genKey(dataType, funcName)
    val cacheFunc = udfCache.getIfPresent(name)
    if (cacheFunc == null) {
      udfCache.put(name, "")
      sparkSession.udf.register(name, new SparderAggFun(funcName, dataType))
    }
    name
  }

  def genKey(dataType: DataType, funcName: String): String = {
    dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName
  }

}

object UdfManager {

  private val defaultManager = new AtomicReference[UdfManager]
  private val defaultSparkSession: AtomicReference[SparkSession] =
    new AtomicReference[SparkSession]

  def refresh(sc: JavaSparkContext): Unit = {
    val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate

    defaultManager.get().destory()
    create(sparkSession)
  }

  def create(sparkSession: SparkSession): Unit = {
    val manager = new UdfManager(sparkSession)
    defaultManager.set(manager)
    defaultSparkSession.set(sparkSession)
  }

  def create(sc: JavaSparkContext): Unit = {
    val sparkSession = SparkSession.builder.config(sc.getConf).getOrCreate
    create(sparkSession)

  }

  def register(dataType: DataType, func: String): String = {
    defaultManager.get().doRegister(dataType, func)
  }

}
