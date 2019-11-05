/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package io.kyligence.kap.engine.spark.job

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
