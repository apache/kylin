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

import scala.collection.JavaConverters._

object LogJobInfoUtils {
  private lazy val infos: BuildJobInfos = KylinBuildEnv.get().buildJobInfos

  def sparkApplicationInfo: String = {
    s"""
       |==========================[BUILD INFO]===============================
       |Override this function to log build info.
       |==========================[BUILD INFO]===============================
     """.stripMargin
  }

  def resourceDetectBeforeCubingJobInfo: String = {
    s"""
       |==========================[RESOURCE DETECT BEFORE CUBE]===============================
       |spark plans :
       |  ${infos.getSparkPlans.asScala.map(_.toString).mkString("\n")}
       |==========================[RESOURCE DETECT BEFORE CUBE]===============================
     """.stripMargin
  }

  def dfBuildJobInfo: String = {
    s"""
       |==========================[BUILD CUBE]===============================
       |auto spark config :${infos.getAutoSparkConfs}
       |wait time: ${infos.waitTime}
       |build time: ${infos.buildTime}
       |build from layouts :
       |${infos.getParent2Children.asScala.filter(_._1 != null)
        .map(entry => s"[layoutId: ${entry._1.getLayoutId}, layoutSize: ${entry._1.getByteSize} bytes," +
          s" children: ${entry._2}]").mkString("\n")}
       |build from flat table :
       |${infos.getParent2Children.asScala.filter(_._1 == null)
        .map(entry => s"[${entry._2}]").mkString("\n")}
       |cuboids num per segment : ${infos.getSeg2cuboidsNumPerLayer}
       |abnormal layouts : ${infos.getAbnormalLayouts}
       |retry times : ${infos.getRetryTimes}
       |job retry infos :
       |  ${infos.getJobRetryInfos.asScala.map(_.toString).mkString("\n")}
       |==========================[BUILD CUBE]===============================
     """.stripMargin
  }

  def resourceDetectBeforeMergingJobInfo: String = {
    s"""
       |==========================[RESOURCE DETECT BEFORE MERGE]===============================
       |merging segments : ${infos.getMergingSegments}
       |spark plans :
       |  ${infos.getSparkPlans.asScala.map(_.toString).mkString("\n")}
       |==========================[RESOURCE DETECT BEFORE MERGE]===============================
     """.stripMargin
  }

  def dfMergeJobInfo: String = {
    s"""
       |==========================[MERGE CUBE]===============================
       |auto spark config : ${infos.getAutoSparkConfs}
       |wait time: ${infos.waitTime}
       |build time: ${infos.buildTime}
       |merging segments : ${infos.getMergingSegments}
       |abnormal layouts : ${infos.getAbnormalLayouts}
       |retry times : ${infos.getRetryTimes}
       |job retry infos :
       |  ${infos.getJobRetryInfos.asScala.map(_.toString).mkString("\n")}
       |==========================[MERGE CUBE]===============================
     """.stripMargin
  }
}
