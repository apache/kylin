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
