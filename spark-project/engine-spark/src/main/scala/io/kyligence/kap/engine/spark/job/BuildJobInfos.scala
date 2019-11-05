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

import java.util

import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree
import io.kyligence.kap.metadata.cube.model.{NDataLayout, NDataSegment}
import org.apache.spark.application.RetryInfo
import org.apache.spark.sql.execution.SparkPlan

class BuildJobInfos {
  // BUILD
  private val seg2cuboidsNumPerLayer: util.Map[String, util.List[Int]] = new util.HashMap[String, util.List[Int]]

  private val seg2SpanningTree: java.util.Map[String, NSpanningTree] = new util.HashMap[String, NSpanningTree]

  private val parent2Children: util.Map[NDataLayout, util.List[Long]] = new util.HashMap[NDataLayout, util.List[Long]]

  // MERGE
  private val sparkPlans: java.util.List[SparkPlan] = new util.LinkedList[SparkPlan]

  private val mergingSegments: java.util.List[NDataSegment] = new util.LinkedList[NDataSegment]

  // COMMON
  private val abnormalLayouts: util.Map[Long, util.List[String]] = new util.HashMap[Long, util.List[String]]

  private var retryTimes = 0

  private val autoSparkConfs: java.util.Map[String, String] = new util.HashMap[String, String]

  private val jobRetryInfos: java.util.List[RetryInfo] = new util.LinkedList[RetryInfo]

  var buildTime: Long = 0L

  private var jobStartTime: Long = 0L

  var waitTime: Long = 0L

  private var waitStartTime: Long = 0L

  def startJob(): Unit = {
    jobStartTime = System.currentTimeMillis()
  }
  def jobEnd(): Unit = {
    buildTime = System.currentTimeMillis() - jobStartTime
  }

  def startWait(): Unit = {
    waitStartTime = System.currentTimeMillis()
  }

  def endWait(): Unit = {
    waitTime = System.currentTimeMillis() - waitStartTime
  }

  def recordSpanningTree(segId: String, tree: NSpanningTree): Unit = {
    seg2SpanningTree.put(segId, tree)
  }

  def getSpanningTree(segId: String): NSpanningTree = {
    seg2SpanningTree.get(segId)
  }

  def recordMergingSegments(segments: util.List[NDataSegment]): Unit = {
    mergingSegments.addAll(segments)
  }

  def clearMergingSegments(): Unit = {
    mergingSegments.clear()
  }

  def getMergingSegments: util.List[NDataSegment] = {
    mergingSegments
  }

  def recordSparkPlan(plan: SparkPlan): Unit = {
    sparkPlans.add(plan)
  }

  def clearSparkPlans(): Unit = {
    sparkPlans.clear()
  }

  def getSparkPlans: util.List[SparkPlan] = {
    sparkPlans
  }

  def getAbnormalLayouts: util.Map[Long, util.List[String]] = {
    abnormalLayouts
  }

  def recordAbnormalLayouts(key: Long, value: String): Unit = {
    if (abnormalLayouts.containsKey(key)) {
      abnormalLayouts.get(key).add(value)
    } else {
      val reasons = new util.LinkedList[String]()
      reasons.add(value)
      abnormalLayouts.put(key, reasons)
    }
  }

  def getAutoSparkConfs: util.Map[String, String] = {
    autoSparkConfs
  }

  def recordAutoSparkConfs(confs: util.Map[String, String]): Unit = {
    autoSparkConfs.putAll(confs)
  }

  def getJobRetryInfos: util.List[RetryInfo] = {
    jobRetryInfos
  }

  def recordJobRetryInfos(info: RetryInfo): Unit = {
    jobRetryInfos.add(info)
  }

  def recordRetryTimes(times: Int): Unit = {
    retryTimes = times
  }

  def getRetryTimes: Int = {
    retryTimes
  }

  def recordCuboidsNumPerLayer(segId: String, num: Int): Unit = {
    if (seg2cuboidsNumPerLayer.containsKey(segId)) {
      seg2cuboidsNumPerLayer.get(segId).add(num)
    } else {
      val nums = new util.LinkedList[Int]()
      nums.add(num)
      seg2cuboidsNumPerLayer.put(segId, nums)
    }
  }

  def clearCuboidsNumPerLayer(segId: String): Unit = {
    if (seg2cuboidsNumPerLayer.containsKey(segId)) {
      seg2cuboidsNumPerLayer.get(segId).clear()
    }
  }

  def getSeg2cuboidsNumPerLayer: util.Map[String, util.List[Int]] = {
    seg2cuboidsNumPerLayer
  }

  def recordParent2Children(key: NDataLayout, value: util.List[Long]): Unit = {
    parent2Children.put(key, value)
  }

  def getParent2Children: util.Map[NDataLayout, util.List[Long]] = {
    parent2Children
  }
}
