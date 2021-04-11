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

import org.apache.spark.SPARK_VERSION

object SparkVersionUtils {

  /**
   * Utility method to return the Spark Versions as float type
   * and only return major version.
   * SPARK_VERSION will be of format x.y.z e.g 2.4.7, 3.0.1,
   * and return 2.4, 3.0
   */
  def getSparkVersionAsNumeric(): Float = {
    val tmpArray = SPARK_VERSION.split("\\.")
    // convert to float
    if (tmpArray.length >= 2) {
      (tmpArray(0) + "." + tmpArray(1)).toFloat
    } else {
      (tmpArray(0) + ".0").toFloat
    }
  }

  /**
   * This API ignores the sub-version and compares with only major version.
   * Version passed should be of format x.y  e.g 2.2 ,2.3.
   */
  def isEqualToSparkVersion(targetVersion: String): Boolean = {
    getSparkVersionAsNumeric == targetVersion.toFloat
  }

  /**
   * This API ignores the sub-version and compares with only major version.
   * Version passed should be of format x.y  e.g 2.2 ,2.3.
   */
  def isGreaterThanSparkVersion(targetVersion: String, isEqualTo: Boolean = false): Boolean = {
    // compare the versions
    if (isEqualTo) {
      getSparkVersionAsNumeric >= targetVersion.toFloat
    } else {
      getSparkVersionAsNumeric > targetVersion.toFloat
    }
  }

  /**
   * This API ignores the sub-version and compares with only major version.
   * Version passed should be of format x.y  e.g 2.2 ,2.3.
   */
  def isLessThanSparkVersion(targetVersion: String, isEqualTo: Boolean = false): Boolean = {
    // compare the versions
    if (isEqualTo) {
      getSparkVersionAsNumeric <= targetVersion.toFloat
    } else {
      getSparkVersionAsNumeric < targetVersion.toFloat
    }
  }
}
