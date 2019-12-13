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
package org.apache.spark.utils

import java.util.concurrent.{ExecutorService, TimeUnit}

import org.apache.spark.SparkException
import org.apache.spark.util.ThreadUtils

import scala.concurrent.Awaitable
import scala.concurrent.duration.{Duration, FiniteDuration}

object ProxyThreadUtils {
  @throws(classOf[SparkException])
  def awaitResult[T](awaitable: Awaitable[T], atMost: Duration): T = {
   ThreadUtils.awaitResult(awaitable, atMost)
  }


  def shutdown(executor: ExecutorService,
                gracePeriod: Duration = FiniteDuration(30, TimeUnit.SECONDS)): Unit = {
    ThreadUtils.shutdown(executor, gracePeriod)
  }
}
