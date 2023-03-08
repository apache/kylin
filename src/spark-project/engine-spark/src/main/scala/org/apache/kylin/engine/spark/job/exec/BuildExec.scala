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

package org.apache.kylin.engine.spark.job.exec

import org.apache.kylin.engine.spark.job.stage.StageExec
import java.io.IOException
import java.util
import java.util.Locale

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._

class BuildExec(var id: String) extends Logging{
  protected var subStages = new util.ArrayList[StageExec]

  def getId: String = {
    this.id
  }

  @throws(classOf[IOException])
  def buildSegment(): Unit = {
    for (stage <- subStages.asScala) {
      logInfo(s"Start sub stage ${stage.getStageName}")
      stage.toWork()
      logInfo(s"End sub stage ${stage.getStageName}")
    }
  }

  def addStage(stage: StageExec): Unit = {
    val stageId = subStages.size + 2

    stage.setId(getId + "_" + String.format(Locale.ROOT, "%02d", Integer.valueOf(stageId)))
    this.subStages.add(stage)
  }
}
