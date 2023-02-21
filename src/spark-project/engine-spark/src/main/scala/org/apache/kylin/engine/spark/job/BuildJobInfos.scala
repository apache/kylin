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

import org.apache.kylin.metadata.cube.cuboid.NSpanningTree
import org.apache.kylin.metadata.cube.model.{NDataLayout, NDataSegment}
import org.apache.spark.application.RetryInfo
import org.apache.spark.sql.execution.SparkPlan

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class BuildJobInfos {
  // BUILD
  private val seg2cuboidsNumPerLayer: util.Map[String, AtomicInteger] = new ConcurrentHashMap[String, AtomicInteger]()

  private val seg2SpanningTree: java.util.Map[String, NSpanningTree] = new util.HashMap[String, NSpanningTree]

  private val parent2Children: util.Map[NDataLayout, util.HashSet[Long]] = new util.HashMap[NDataLayout, util.HashSet[Long]]

  // MERGE
  private val sparkPlans: java.util.List[SparkPlan] = new util.LinkedList[SparkPlan]

  private val mergingSegments: java.util.List[NDataSegment] = new util.LinkedList[NDataSegment]

  // BUCKET
  private val bucketsInfo = new util.HashMap[Long, util.List[Long]]

  private var bucketingSegment: String = ""

  // COMMON
  private val abnormalLayouts: util.Map[Long, util.List[String]] = new util.HashMap[Long, util.List[String]]

  private var retryTimes = 0

  private val autoSparkConfs: java.util.Map[String, String] = new util.HashMap[String, String]

  private val jobRetryInfos: java.util.List[RetryInfo] = new util.LinkedList[RetryInfo]

  var buildTime: Long = 0L

  private var jobStartTime: Long = 0L

  var waitTime: Long = 0L

  private var waitStartTime: Long = 0L

  private var jobStepId = ""

  private var segmentId = ""

  private var stageId = ""

  private var jobId = ""

  private var project = ""

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
    var counter = seg2cuboidsNumPerLayer.get(segId)
    if (counter == null) {
      seg2cuboidsNumPerLayer.putIfAbsent(segId, new AtomicInteger())
      counter = seg2cuboidsNumPerLayer.get(segId)
    }
    counter.addAndGet(num)
  }

  def clearCuboidsNumPerLayer(segId: String): Unit = {
    seg2cuboidsNumPerLayer.remove(segId)
  }

  def getSeg2cuboidsNumPerLayer: util.Map[String, AtomicInteger] = {
    seg2cuboidsNumPerLayer
  }

  def recordParent2Children(key: NDataLayout, value: util.HashSet[Long]): Unit = {
    parent2Children.put(key, value)
  }

  def getParent2Children: util.Map[NDataLayout, util.HashSet[Long]] = {
    parent2Children
  }

  def recordBucketsInfo(bucketId: Long, partitions: util.List[Long]): Unit = {
    bucketsInfo.put(bucketId, partitions)
  }

  def getBucketsInfo(): Unit = {
    bucketsInfo
  }

  def recordBucketSegment(segId: String): Unit = {
    bucketingSegment = segId
  }

  def getBucketSegment(): Unit = {
    bucketingSegment
  }

  def recordJobStepId(stepId: String): Unit = {
    jobStepId = stepId
  }

  def getJobStepId: String = {
    jobStepId
  }

  def recordSegmentId(errSegmentId: String): Unit = {
    segmentId = errSegmentId
  }

  def getSegmentId: String = {
    segmentId
  }

  def recordStageId(errStageId: String): Unit = {
    stageId = errStageId
  }

  def getStageId: String = {
    stageId
  }

  def recordJobId(recordJobId: String): Unit = {
    jobId = recordJobId
  }

  def getJobId: String = {
    jobId
  }

  def recordProject(recordProject: String): Unit = {
    project = recordProject
  }

  def getProject: String = {
    project
  }

  def clear(): Unit = {
    seg2cuboidsNumPerLayer.clear()
    seg2SpanningTree.clear()
    parent2Children.clear()
    sparkPlans.clear()
    mergingSegments.clear()
    abnormalLayouts.clear()
    autoSparkConfs.clear()
    jobRetryInfos.clear()
  }
}
