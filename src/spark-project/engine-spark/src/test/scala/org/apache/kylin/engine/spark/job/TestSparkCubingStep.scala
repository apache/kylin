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

package org.apache.kylin.engine.spark.job

import org.apache.kylin.job.execution.AbstractExecutable
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}

class TestSparkCubingStep extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  test("auto set driver memory by cuboid num") {
    assert(AbstractExecutable.computeDriverMemory(1) == 1024)
    assert(AbstractExecutable.computeDriverMemory(2) == 1024)
    assert(AbstractExecutable.computeDriverMemory(3) == 2048)
    assert(AbstractExecutable.computeDriverMemory(4) == 2048)
    assert(AbstractExecutable.computeDriverMemory(10) == 2048)
    assert(AbstractExecutable.computeDriverMemory(15) == 2048)
    assert(AbstractExecutable.computeDriverMemory(20) == 2048)
    assert(AbstractExecutable.computeDriverMemory(25) == 3072)
    assert(AbstractExecutable.computeDriverMemory(40) == 3072)
    assert(AbstractExecutable.computeDriverMemory(50) == 3072)
    assert(AbstractExecutable.computeDriverMemory(70) == 3072)
    assert(AbstractExecutable.computeDriverMemory(110) == 4096)
  }
}
