/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{SparkConf, SparkEnv}

/**
 * Convenience utility object for invoking [[SparkHadoopUtil]].
 */
object SparkHadoopUtils {

  /**
   * Simply return a new default hadoop [[Configuration]].
   * @return Newly created default hadoop configuration
   */
  def newConfiguration(): Configuration = {
    new Configuration()
  }

  /**
   * Returns a new hadoop [[Configuration]] with current [[SparkConf]] from [[SparkEnv]].
   * @return Newly created hadoop configuration with extra spark properties
   */
  def newConfigurationWithSparkConf(): Configuration = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) {
      throw new IllegalStateException("sparkEnv should not be null")
    }
    SparkHadoopUtil.newConfiguration(sparkEnv.conf)
  }

  /**
   * Returns a new hadoop [[Configuration]] with [[SparkConf]] given.
   * @param sparkConf A [[SparkConf]]
   * @return Newly created hadoop configuration with extra spark properties given
   */
  def newConfigurationWithSparkConf(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.newConfiguration(sparkConf)
  }
}
