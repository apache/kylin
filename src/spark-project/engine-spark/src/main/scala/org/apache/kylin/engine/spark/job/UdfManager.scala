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

package org.apache.kylin.engine.spark.job

import com.esotericsoftware.kryo.io.{Input, KryoDataInput}
import org.apache.kylin.guava30.shaded.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{FunctionEntity, KapFunctions, SparkSession}
import org.roaringbitmap.longlong.Roaring64NavigableMap

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class UdfManager(sparkSession: SparkSession) extends Logging {
  private var udfCache: Cache[String, String] = _

  KapFunctions.builtin.foreach { case FunctionEntity(name, info, builder) =>
      sparkSession.sessionState.functionRegistry.registerFunction(name, info, builder)
    }

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

  sparkSession.sessionState.udfRegistration.register("eval_bitmap", (arg1: Array[Byte]) => {
    if (arg1.nonEmpty) {
      val bitMap = new Roaring64NavigableMap()
      bitMap.deserialize(new KryoDataInput(new Input(arg1)))
      bitMap.getLongCardinality
    } else {
      0L
    }
  })

  def destory(): Unit = {
    udfCache.cleanUp()
  }

  def doRegister(dataType: DataType, funcName: String, schema: StructType, isFirst: Boolean): String = {
    val name = genKey(dataType, funcName, isFirst, schema)
    val cacheFunc = udfCache.getIfPresent(name)
    if (cacheFunc == null) {
      if (funcName == "TOP_N") {
        sparkSession.udf.register(name, new TopNUDAF(dataType, schema, isFirst))
      } else {
        sparkSession.udf.register(name, new FirstUDAF(funcName, dataType, isFirst))
      }
      udfCache.put(name, "")
    }
    name
  }

  def genKey(dataType: DataType, funcName: String, isFirst: Boolean, schema: StructType): String = {
    val key = dataType.toString
      .replace("(", "_")
      .replace(")", "_")
      .replace(",", "_") + funcName + "_" + isFirst
    if (funcName == "TOP_N") {
      s"${key}_${schema.mkString}"
    } else {
      key
    }
  }

}

object UdfManager {

  private val defaultManager = new AtomicReference[UdfManager]
  private val defaultSparkSession: AtomicReference[SparkSession] =
    new AtomicReference[SparkSession]

  def create(sparkSession: SparkSession): Unit = {
    val manager = new UdfManager(sparkSession)
    defaultManager.set(manager)
    defaultSparkSession.set(sparkSession)
  }

  def register(dataType: DataType, func: String, schema: StructType, isFirst: Boolean): String = {
    defaultManager.get().doRegister(dataType, func, schema, isFirst)
  }
}
