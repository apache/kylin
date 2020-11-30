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

import org.apache.spark.SparkConf

object SparderUtils {

  def getTotalCore(sparkConf: SparkConf): Int = {
    if (sparkConf.get("spark.master").startsWith("local")) {
      return 1
    }
    val instances = getExecutorNum(sparkConf)
    val cores = sparkConf.get("spark.executor.cores").toInt
    Math.max(instances * cores, 1)
  }

  def getExecutorNum(sparkConf: SparkConf): Int = {
    if (sparkConf.get("spark.dynamicAllocation.enabled", "false").toBoolean) {
      val maxExecutors = sparkConf.get("spark.dynamicAllocation.maxExecutors", Int.MaxValue.toString).toInt
      maxExecutors
    } else {
      sparkConf.get("spark.executor.instances").toInt
    }
  }
}
