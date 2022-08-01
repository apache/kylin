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

package org.apache.spark.sql.udf

import org.apache.spark.unsafe.types.UTF8String

object SplitPartImpl {

  def evaluate(str: String, rex: String, index: Int): UTF8String = {
    val parts = str.split(rex)
    if (index - 1 < parts.length && index > 0) {
      UTF8String.fromString(parts(index - 1))
    } else if (index < 0 && Math.abs(index) <= parts.length) {
      UTF8String.fromString(parts(parts.length + index))
    } else {
      null
    }
  }
}
