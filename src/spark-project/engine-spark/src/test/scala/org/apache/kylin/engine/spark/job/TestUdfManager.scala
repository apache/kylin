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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll

class TestUdfManager extends SparderBaseFunSuite with SharedSparkSession with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    UdfManager.create(spark)
  }

  // ignore for dev, if you want to run this case, modify udfCache maximum to Long.MaxValue
  // and if (funcName == "TOP_N") to if (funcName.startsWith("TOP_N")) in io.kyligence.kap.engine.spark.job.UdfManager
  ignore("test register udf in multi-thread") {
    import functions.udf
    val testFunc = udf(() => "test")

    // try best to block registering udf to spark in thread2, increase probability of concurrency issue
    new Thread(new Runnable {
      override def run(): Unit = {
        var count = 0
        while (true) {
          spark.udf.register(s"test_$count", testFunc)
          count += 1
        }
      }
    }).start()

    // thread2 and thread3 are combined to mock registering udf in multi-threading
    val latch2 = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        latch2.await()
        var count = 0
        while (true) {
          UdfManager.register(DataType.getType("int"), s"TOP_N_$count",
            new StructType(Array(StructField("test_column", StringType))), isFirst = true)
          count += 1
        }
      }
    }).start()

    val latch = new CountDownLatch(1)
    new Thread(new Runnable {
      override def run(): Unit = {
        latch2.await()
        var count = 0
        while (true) {
          val name = UdfManager.register(DataType.getType("int"), s"TOP_N_$count",
            new StructType(Array(StructField("test_column", StringType))), isFirst = true)
          if (!spark.sessionState.functionRegistry
            .functionExists(FunctionIdentifier.apply(name))) {
            latch.countDown()
          }
          count += 1
        }
      }
    }).start()

    latch2.countDown()
    if (latch.await(2, TimeUnit.SECONDS)) {
      fail()
    }
  }
}
