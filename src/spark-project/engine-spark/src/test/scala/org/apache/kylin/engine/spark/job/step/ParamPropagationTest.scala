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

package org.apache.kylin.engine.spark.job.step

import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Objects
import scala.collection.mutable

class ParamPropagationTest extends AnyFunSuite {
  test("basic") {
    val params = new ParamPropagation()
    assert(!params.isSkipPersistFlatTable)

    params.setSkipPersistFlatTable(true)
    assert(params.isSkipPersistFlatTable)

    params.setSkipPersistFlatTable(false)
    assert(!params.isSkipPersistFlatTable)

    assert(!params.isSkipPersistFactView)

    params.setSkipPersistFactView(true)
    assert(params.isSkipPersistFactView)

    params.setSkipPersistFactView(false)
    assert(!params.isSkipPersistFactView)

    params.setCachedLayouts(mutable.HashMap[Long, Dataset[Row]]())
    assert(Objects.nonNull(params.getCachedLayouts))

    params.setCachedLayoutPartitions(mutable.HashMap[Long, mutable.HashMap[Long, Dataset[Row]]
    ]())
    assert(Objects.nonNull(params.getCachedLayoutPartitions))

    params.setCachedLayoutSanity(Some(Map(1L -> 10001L)))
    assert(params.getCachedLayoutSanity.isDefined)

    params.setCachedIndexInferior(Some(Map(1L -> InferiorGroup(null, null))))
    assert(params.getCachedIndexInferior.isDefined)
    assert(Objects.isNull(params.getCachedIndexInferior.get(1L).tableDS))
    assert(Objects.isNull(params.getCachedIndexInferior.get(1L).reapCount))
    val ig = InferiorGroup.unapply(params.getCachedIndexInferior.get(1L)).get
    assert(Objects.isNull(ig._1))
    assert(Objects.isNull(ig._2))
  }
}
