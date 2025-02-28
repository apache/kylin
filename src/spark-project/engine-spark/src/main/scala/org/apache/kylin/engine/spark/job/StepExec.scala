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

import org.apache.kylin.engine.spark.job.step.StageExec
import org.apache.spark.internal.Logging

import java.io.IOException
import java.util
import java.util.Locale
import scala.collection.JavaConverters._

class StepExec(stepId: String) extends Logging {
  protected var subStageList = new util.ArrayList[StageExec]

  def getStageId(): Int = {
    subStageList.size + 1
  }

  def addStage(stage: StageExec): Unit = {
    val stageId = getStageId()
    stage.setStageId(getStepId + "_" + String.format(Locale.ROOT, "%02d", Integer.valueOf(stageId)))
    this.subStageList.add(stage)
  }

  def getStepId: String = {
    this.stepId
  }

  @throws(classOf[IOException])
  def doExecute(): Unit = {
    if (subStageList.size() > 1) {
      subStageList.asScala.foreach(_.doExecute())
    } else {
      subStageList.asScala.foreach(_.doExecuteWithoutFinally())
    }
  }
}
