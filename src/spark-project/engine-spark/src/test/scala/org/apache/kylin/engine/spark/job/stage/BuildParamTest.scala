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

package org.apache.kylin.engine.spark.job.stage

import java.util.Objects

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable

class BuildParamTest extends AnyFunSuite {
  test("basic") {
    val param = new BuildParam()
    assert(!param.isSkipGenerateFlatTable)

    param.setSkipGenerateFlatTable(true)
    assert(param.isSkipGenerateFlatTable)

    param.setSkipGenerateFlatTable(false)
    assert(!param.isSkipGenerateFlatTable)

    assert(!param.isSkipMaterializedFactTableView)

    param.setSkipMaterializedFactTableView(true)
    assert(param.isSkipMaterializedFactTableView)

    param.setSkipMaterializedFactTableView(false)
    assert(!param.isSkipMaterializedFactTableView)

    param.setCachedLayoutDS(mutable.HashMap[Long, Dataset[Row]]())
    assert(Objects.nonNull(param.getCachedLayoutDS))

    param.setCachedLayoutPartitionDS(mutable.HashMap[Long, mutable.HashMap[Long, Dataset[Row]]
    ]())
    assert(Objects.nonNull(param.getCachedLayoutPartitionDS))

    param.setCachedLayoutSanity(Some(Map(1L -> 10001L)))
    assert(param.getCachedLayoutSanity.isDefined)

    param.setCachedIndexInferior(Some(Map(1L -> InferiorGroup(null, null))))
    assert(param.getCachedIndexInferior.isDefined)
    assert(Objects.isNull(param.getCachedIndexInferior.get(1L).tableDS))
    assert(Objects.isNull(param.getCachedIndexInferior.get(1L).reapCount))
    val ig = InferiorGroup.unapply(param.getCachedIndexInferior.get(1L)).get
    assert(Objects.isNull(ig._1))
    assert(Objects.isNull(ig._2))
  }
}
