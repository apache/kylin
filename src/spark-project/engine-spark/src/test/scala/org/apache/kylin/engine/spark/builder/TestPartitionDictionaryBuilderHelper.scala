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

package org.apache.kylin.engine.spark.builder

import org.apache.kylin.guava30.shaded.common.collect.{Lists, Sets}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.cube.model.{IndexEntity, NDataSegDetails, NDataSegment}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.mockito.Mockito

class TestPartitionDictionaryBuilderHelper extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {
  private val DEFAULT_PROJECT = "default"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  private val MODEL_NAME2 = "a8ba3ff1-83bd-4066-ad54-d2fb3d1f0e94"
  private val MODEL_NAME3 = "741ca86a-1f13-46da-a59f-95fb68615e3a"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("test extractTreeRelatedGlobalDictToBuild mock") {
    val segment = Mockito.mock(classOf[NDataSegment])

    val toBuildIndexEntities = Sets.newHashSet[IndexEntity]
    val indexEntity = new IndexEntity()
    indexEntity.setLayouts(Lists.newArrayList())
    toBuildIndexEntities.add(indexEntity)

    Mockito.when(segment.getSegDetails).thenReturn(null)

    var result = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(segment, toBuildIndexEntities)
    assert(result.isEmpty)

    Mockito.when(segment.getSegDetails).thenReturn(new NDataSegDetails())
    result = PartitionDictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(segment, toBuildIndexEntities)
    assert(result.isEmpty)
  }
}
