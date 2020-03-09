/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
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
 */

package org.apache.spark.memory

import org.apache.spark.SparkEnv

class SparkMemoryManagerHandle(
  val onHeapStorage: StorageMemoryPool,
  val offHeapStorage: StorageMemoryPool,
  val onHeapExecution: ExecutionMemoryPool,
  val offHeapExecution: ExecutionMemoryPool,
  val lock: Object) extends MemoryGetter {

  val names: Seq[String] = for {
    t <- Seq("onHeap", "offHeap")
    s <- Seq("Storage", "Execution", "MaxTaskExecution")
  } yield {
    (s"$t$s")
  }

  override def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = onHeapStorage.memoryUsed
    dest(offset + 1) = onHeapExecution.memoryUsed
    dest(offset + 2) = maxTaskExecution(onHeapExecution)
    dest(offset + 3) = offHeapStorage.memoryUsed
    dest(offset + 4) = offHeapExecution.memoryUsed
    dest(offset + 5) = maxTaskExecution(offHeapExecution)
  }

  def maxTaskExecution(executionMemoryPool: ExecutionMemoryPool): Long = {
    import Reflector._
    lock.synchronized {
      val taskMem = executionMemoryPool
        .reflectField("memoryForTask").asInstanceOf[scala.collection.Map[Long, Long]]
      if (taskMem.nonEmpty) {
        taskMem.values.max
      } else {
        0L
      }
    }
  }
}

object SparkMemoryManagerHandle {
  def get(): Option[SparkMemoryManagerHandle] = try {
    val env = SparkEnv.get
    val memManager = env.memoryManager
    import Reflector._
    Some(new SparkMemoryManagerHandle(
      memManager.reflectField("onHeapStorageMemoryPool").asInstanceOf[StorageMemoryPool],
      memManager.reflectField("offHeapStorageMemoryPool").asInstanceOf[StorageMemoryPool],
      memManager.reflectField("onHeapExecutionMemoryPool").asInstanceOf[ExecutionMemoryPool],
      memManager.reflectField("offHeapExecutionMemoryPool").asInstanceOf[ExecutionMemoryPool],
      memManager
    ))
  } catch {
    case ex: Exception =>
      ex.printStackTrace()
      None
  }
}
