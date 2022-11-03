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

package io.kyligence.kap.softaffinity.strategy

import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SoftAffinityStrategy extends SoftAffinityAllocationTrait with Logging {

  /**
   * allocate target executors for file
   */
  override def allocateExecs(file: String,
                             candidates: ListBuffer[Option[(String, String)]]): Array[(String, String)] = {
    if (candidates.size < 1) {
      Array.empty
    } else {
      val candidatesSize = candidates.size
      val halfCandidatesSize = candidatesSize / softAffinityReplicationNum
      val resultSet = new mutable.LinkedHashSet[(String, String)]

      val mod = file.hashCode % candidatesSize
      val c1 = if (mod < 0) mod + candidatesSize else mod
      // check whether the executor with index c1 is down
      if (candidates(c1).isDefined) {
        resultSet.add(candidates(c1).get)
      }
      for (i <- 1 until softAffinityReplicationNum) {
        val c2 = (c1 + halfCandidatesSize + i) % candidatesSize
        if (candidates(c2).isDefined) {
          resultSet.add(candidates(c2).get)
        }
      }
      resultSet.toArray
    }
  }
}