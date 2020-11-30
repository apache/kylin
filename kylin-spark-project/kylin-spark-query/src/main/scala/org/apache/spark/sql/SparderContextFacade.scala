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

import org.apache.kylin.common.threadlocal.InternalThreadLocal
import org.apache.kylin.common.util.Pair
import org.apache.kylin.query.UdfManager

object SparderContextFacade extends Logging {

  final val CURRENT_SPARKSESSION: InternalThreadLocal[Pair[SparkSession, UdfManager]] =
    new InternalThreadLocal[Pair[SparkSession, UdfManager]]()

  def current(): Pair[SparkSession, UdfManager] = {
    if (CURRENT_SPARKSESSION.get() == null) {
      val spark = SparderContext.getOriginalSparkSession.cloneSession()
      CURRENT_SPARKSESSION.set(new Pair[SparkSession, UdfManager](spark,
        UdfManager.createWithoutBuildInFunc(spark)))
    }
    CURRENT_SPARKSESSION.get()
  }

  def remove(): Unit = {
    CURRENT_SPARKSESSION.remove()
  }
}