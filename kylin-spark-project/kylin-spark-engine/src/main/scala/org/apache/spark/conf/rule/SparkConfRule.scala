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

package org.apache.spark.conf.rule


import org.apache.kylin.engine.spark.utils.{SparkConfHelper, SparkConfRuleConstants}
import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

sealed trait SparkConfRule extends Logging {
  def apply(helper: SparkConfHelper): Unit = {
    try {
      doApply(helper)
    } catch {
      case throwable: Throwable =>
        logWarning(s"Apply rule error for rule ${this.getClass.getName}", throwable)
        fallback(helper: SparkConfHelper)
    }
  }

  def doApply(helper: SparkConfHelper): Unit

  def fallback(helper: SparkConfHelper): Unit = {

  }
}

class ExecutorMemoryRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val hasCountDistinct = helper.hasCountDistinct
    val memory = sourceGB match {
      case _ if `sourceGB` >= 100 && `hasCountDistinct` =>
        "20GB"
      case _ if (`sourceGB` >= 100) || (`sourceGB` >= 10 && `hasCountDistinct`) =>
        "16GB"
      case _ if `sourceGB` >= 10 || (`sourceGB` >= 1 && `hasCountDistinct`) =>
        "10GB"
      case _ if `sourceGB` >= 1 || `hasCountDistinct` =>
        "4GB"
      case _ =>
        "1GB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, memory)
  }
}

class ExecutorCoreRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val hasCountDistinct = helper.hasCountDistinct
    val cores = if (sourceGB >= 1 || hasCountDistinct) {
      "5"
    } else {
      SparkConfRuleConstants.DEFUALT_EXECUTOR_CORE
    }
    helper.setConf(SparkConfHelper.EXECUTOR_CORES, cores)
  }
}

class ExecutorOverheadRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceGB = Utils.byteStringAsGb(helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE))
    val hasCountDistinct = helper.hasCountDistinct
    val overhead = sourceGB match {
      case _ if `sourceGB` >= 100 && `hasCountDistinct` =>
        "6GB"
      case _ if (`sourceGB` >= 100) || (`sourceGB` >= 10 && `hasCountDistinct`) =>
        "4GB"
      case _ if `sourceGB` >= 10 || (`sourceGB` >= 1 && `hasCountDistinct`) =>
        "2GB"
      case _ if `sourceGB` >= 1 || `hasCountDistinct` =>
        "1GB"
      case _ =>
        "512MB"
    }
    helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, overhead)
  }
}

class ExecutorInstancesRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val queue = helper.getConf(SparkConfHelper.DEFAULT_QUEUE)
    val layoutSize = helper.getOption(SparkConfHelper.LAYOUT_SIZE)
    val requiredCores = helper.getOption(SparkConfHelper.REQUIRED_CORES)
    logInfo(s"RequiredCores is $requiredCores")
    val baseExecutorInstances = KylinConfig.getInstanceFromEnv.getSparkEngineBaseExecutorInstances
    val calculateExecutorInsByLayoutSize = calculateExecutorInstanceSizeByLayoutSize(Integer.parseInt(layoutSize))
    logInfo(s"The number of instances calculated by cuboids " +
      s"is: $calculateExecutorInsByLayoutSize")

    var availableMem = Int.MaxValue
    try {
      availableMem = helper.getFetcher.fetchQueueAvailableResource(queue).available.memory
    } catch {
      case throwable: Throwable =>
        logWarning(s"Error when getting available memory from cluster, ignore it.")
    }
    var availableCore = Int.MaxValue
    try {
      availableCore = helper.getFetcher.fetchQueueAvailableResource(queue).available.vCores
    } catch {
      case throwable: Throwable =>
        logWarning(s"Error when getting available cpus from cluster, ignore it.")
    }
    val executorMem = (Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_MEMORY)) +
      Utils.byteStringAsMb(helper.getConf(SparkConfHelper.EXECUTOR_OVERHEAD)))

    val executorCore: Int = Option(helper.getConf(SparkConfHelper.EXECUTOR_CORES)) match {
      case Some(cores) => cores.toInt
      case None => SparkConfRuleConstants.DEFUALT_EXECUTOR_CORE.toInt
    }

    logInfo(s"Current availableMem on yarn is $availableMem, " +
      s"availableCore is $availableCore")
    logInfo(s"Current instance memory is set to $executorMem, " +
      s"cores is set to $executorCore")
    val queueAvailableInstance = Math.min(availableMem / executorMem, availableCore / executorCore)

    val needInstance = Math.max(calculateExecutorInsByLayoutSize.toLong, requiredCores.toInt / executorCore)
    val instance = Math.min(needInstance, queueAvailableInstance)
    val executorInstance = Math.max(instance.toLong, baseExecutorInstances.toLong).toString
    logInfo(s"Current queueAvailableInstance is $queueAvailableInstance, " +
      s"needInstance is $needInstance, instance is $instance")
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, executorInstance)
  }

  override def fallback(helper: SparkConfHelper): Unit = {
    helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, KylinConfig.getInstanceFromEnv.getSparkEngineBaseExecutorInstances.toString)
  }


  def calculateExecutorInstanceSizeByLayoutSize(layoutSize: Int): Int = {
    logInfo(s"Calculate the number of executor instance size based on the number of layouts: $layoutSize")
    val config: KylinConfig = KylinConfig.getInstanceFromEnv
    val baseInstances: Integer = config.getSparkEngineBaseExecutorInstances
    var instanceMultiple = 1

    if (layoutSize != -1) {
      val instanceStrategy: String = config.getSparkEngineExecutorInstanceStrategy
      val tuple = instanceStrategy.split(",")
        .zipWithIndex
        .partition(tp => tp._2 % 2 == 0)

      val choosen = tuple._1
        .map(_._1.toInt)
        .zip(tuple._2.map(_._1.toInt))
        .filter(tp => tp._1 <= layoutSize)
        .lastOption

      if (choosen != None) {
        instanceMultiple = choosen.last._2.toInt
      }
    }
    logInfo(s"The instanceMultiple is $instanceMultiple")
    baseInstances * instanceMultiple
  }
}

class ShufflePartitionsRule extends SparkConfRule {
  override def doApply(helper: SparkConfHelper): Unit = {
    val sourceTableSize = helper.getOption(SparkConfHelper.SOURCE_TABLE_SIZE)
    val partitions = Math.max(2, Utils.byteStringAsMb(sourceTableSize) / 32).toString
    helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, partitions)
  }
}