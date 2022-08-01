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

package org.apache.kylin.query.runtime.plan

import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

class ResultTypeTest extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {
  test("Test normalize") {
    val strSeq = Seq(";select 123,count(1)where=sum(123)or{456}and\\nif\\t")
    val result = ResultPlan.normalize(strSeq)
    Assert.assertEquals(strSeq.length, result.length)
    val except = strSeq.head
    val actual = result.head
    Assert.assertEquals(except.length - 2, actual.length)
    Assert.assertEquals("_select_123_count_1_where_sum_123_or_456_and_if_", actual)
  }
}
