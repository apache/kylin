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

package org.apache.kylin.engine.spark.utils

class JobMetrics {

  private var metrics: Map[String, Long] = Map.empty

  def getMetrics(key: String): Long = {
    metrics.getOrElse(key, -1)
  }

  def setMetrics(key: String, value: Long): Unit = {
    metrics += (key -> value)
  }

  def isDefinedAt(key: String): Boolean = {
    metrics.isDefinedAt(key)
  }

  override def toString: String = {
    s"CuboidRowsCnt is ${metrics.getOrElse(Metrics.CUBOID_ROWS_CNT, null)}, " +
      s"sourceRowsCnt is ${metrics.getOrElse(Metrics.SOURCE_ROWS_CNT, null)}."
  }
}

object Metrics {
  val CUBOID_ROWS_CNT: String = "cuboidRowsCnt"
  val SOURCE_ROWS_CNT: String = "sourceRowsCnt"
}
