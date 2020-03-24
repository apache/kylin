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
