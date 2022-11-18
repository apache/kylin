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

package org.apache.kylin.streaming.app

import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import io.kyligence.kap.guava20.shaded.common.base.Preconditions
import org.apache.kylin.metadata.cube.cuboid.{NSpanningTree, NSpanningTreeFactory}
import org.apache.kylin.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflow, NDataflowManager}
import org.apache.kylin.metadata.cube.utils.StreamingUtils
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.streaming.CreateStreamingFlatTable
import org.apache.kylin.streaming.common.{BuildJobEntry, CreateFlatTableEntry, MicroBatchEntry}
import org.apache.kylin.streaming.jobs.{StreamingDFBuildJob, StreamingJobUtils, StreamingSegmentManager}
import org.apache.kylin.streaming.jobs.StreamingDFBuildJob
import org.apache.kylin.streaming.manager.StreamingJobManager
import org.apache.kylin.streaming.metadata.StreamingJobMeta
import org.apache.kylin.streaming.request.StreamingJobStatsRequest
import org.apache.kylin.streaming.util.JobKiller
import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions => F}
import org.apache.spark.storage.StorageLevel

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.{Locale, TimeZone}
import scala.collection.mutable.ArrayBuffer

object StreamingEntry
  extends Logging {
  var entry: StreamingEntry = _

  def main(args: Array[String]): Unit = {
    entry = new StreamingEntry()
    entry.execute(args)
  }

  def stop(): Unit = {
    if (entry != null) {
      entry.setStopFlag(true)
    }
  }
}

class StreamingEntry
  extends StreamingBuildApplication with Logging {
  val tableRefreshAcc = new AtomicLong()
  val rateTriggerDuration: Long = TimeUnit.MINUTES.toMillis(1)
  val minMaxBuffer = new ArrayBuffer[(Long, Long)](1)
  val gracefulStop: AtomicBoolean = new AtomicBoolean(false)
  lazy val dataflow: NDataflow = NDataflowManager.getInstance(kylinConfig, project).getDataflow(dataflowId)
  lazy val trigger: Trigger = if (kylinConfig.getTriggerOnce) Trigger.Once() else Trigger.ProcessingTime(durationSec * 1000)

  def doExecute(): Unit = {
    log.info("StreamingEntry:{}, {}, {}, {}", project, dataflowId, String.valueOf(durationSec), distMetaUrl)
    Preconditions.checkState(NProjectManager.getInstance(kylinConfig).getProject(project) != null,
      s"metastore can not find this project %s", project)

    registerStreamListener()

    val (query, timeColumn, streamFlatTable) = generateStreamQueryForOneModel()
    Preconditions.checkState(query != null, s"generate query for one model failed for project:  $project dataflowId: %s", dataflowId)
    Preconditions.checkState(timeColumn != null,
      s"streaming query must have time partition column for project:  $project dataflowId: %s", dataflowId)

    val builder = startRealtimeBuildStreaming(streamFlatTable, timeColumn, query)
    addShutdownListener()

    startTableRefreshThread(streamFlatTable)

    while (!ss.sparkContext.isStopped) {
      if (getStopFlag()) {
        ss.streams.active.foreach(_.stop())
        builder.shutdown()
        setStopFlag(false)
        closeSparkSession()
      } else {
        ss.streams.awaitAnyTermination(10000)
      }
    }

    closeAuditLogStore(ss)
    systemExit(0)
  }

  def startRealtimeBuildStreaming(streamFlatTable: CreateStreamingFlatTable, timeColumn: String,
                                  query: Dataset[Row]):
  StreamingDFBuildJob = {
    val nSpanningTree = createSpanningTree(dataflow)

    logInfo(s"start query for model : ${streamFlatTable.model().toString}")
    val builder = new StreamingDFBuildJob(project)
    query
      .writeStream
      .option("checkpointLocation", baseCheckpointLocation + "/" + streamFlatTable.model().getId)
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        // field time have overlap
        val microBatchEntry = new MicroBatchEntry(batchDF, batchId, timeColumn, streamFlatTable,
          dataflow, nSpanningTree, builder, null)
        processMicroBatch(microBatchEntry, minMaxBuffer)
        refreshTable(streamFlatTable)
      }
      .queryName("StreamingEntry")
      .trigger(trigger)
      .start()
    builder
  }

  def processMicroBatch(microBatchEntry: MicroBatchEntry, minMaxBuffer: ArrayBuffer[(Long, Long)]): Unit = {
    val batchDF = microBatchEntry.batchDF.persist(StorageLevel.MEMORY_AND_DISK)
    val flatTableCount: Long = batchDF.count()
    val timeColumn = microBatchEntry.timeColumn
    val minMaxTime = batchDF
      .agg(F.min(F.col(timeColumn)), F.max(F.col(timeColumn)))
      .collect()
      .head
    val batchId = microBatchEntry.batchId
    logInfo(s"start process batch: ${batchId} minMaxTime is ${minMaxTime}")
    if (minMaxTime.getTimestamp(0) != null && minMaxTime.getTimestamp(1) != null) {
      val (minTime, maxTime) = (minMaxTime.getTimestamp(0).getTime, minMaxTime.getTimestamp(1).getTime)
      minMaxBuffer.append((minTime, maxTime))
      val batchSeg = StreamingSegmentManager.allocateSegment(ss, microBatchEntry.sr, dataflowId, project, minTime, maxTime)
      if (batchSeg != null && !StringUtils.isEmpty(batchSeg.getId)) {
        microBatchEntry.streamFlatTable.seg = batchSeg
        val encodedStreamDataset = microBatchEntry.streamFlatTable.encodeStreamingDataset(batchSeg,
          microBatchEntry.df.getModel, batchDF)
        val batchBuildJob = new BuildJobEntry(
          ss,
          project,
          dataflowId,
          flatTableCount,
          batchSeg,
          encodedStreamDataset,
          microBatchEntry.nSpanningTree
        )
        logInfo(s"start build streaming segment: ${batchSeg.toString} spark batchId: ${batchId}")
        logInfo(batchBuildJob.toString)
        microBatchEntry.builder.streamBuild(batchBuildJob)
        logInfo(s"end build streaming segment: ${batchSeg.toString} spark batchId: ${batchId}")
      }
    }
    batchDF.unpersist(true)
  }

  def generateStreamQueryForOneModel():
  (Dataset[Row], String, CreateStreamingFlatTable) = {
    val originConfig = KylinConfig.getInstanceFromEnv

    val parserName = dataflow.getModel.getRootFactTable.getTableDesc.getKafkaConfig.getParserName
    val flatTableDesc = new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan)
    val nSpanningTree = createSpanningTree(dataflow)
    val partitionColumn = NSparkCubingUtil.convertFromDot(dataflow.getModel.getPartitionDesc.getBackTickPartitionDateColumn)
    val flatTableEntry = new CreateFlatTableEntry(flatTableDesc, null, nSpanningTree, ss, null, partitionColumn, watermark, parserName)
    val flatTable = CreateStreamingFlatTable(flatTableEntry)

    val streamingJobMgr = StreamingJobManager.getInstance(originConfig, project)
    val jobMeta: StreamingJobMeta = streamingJobMgr.getStreamingJobByUuid(jobId)
    val config = StreamingJobUtils.getStreamingKylinConfig(originConfig, getJobParams(jobMeta), dataflowId, project)
    val flatDataset = flatTable.generateStreamingDataset(config)
    (flatDataset, partitionColumn, flatTable)
  }

  def createSpanningTree(dataflow: NDataflow): NSpanningTree = {
    val layouts = StreamingUtils.getToBuildLayouts(dataflow)
    Preconditions.checkState(CollectionUtils.isNotEmpty(layouts), "layouts is empty", layouts)
    NSpanningTreeFactory.fromLayouts(layouts, dataflowId)
  }

  def startTableRefreshThread(streamFlatTable: CreateStreamingFlatTable): Unit = {
    if (!streamFlatTable.shouldRefreshTable()) {
      return
    }

    val tableRefreshThread = new Thread() {
      override def run(): Unit = {
        while (isRunning) {
          tableRefreshAcc.getAndAdd(1)
          StreamingUtils.sleep(rateTriggerDuration)
        }
      }
    }
    tableRefreshThread.setDaemon(true)
    tableRefreshThread.start()
  }

  def refreshTable(streamFlatTable: CreateStreamingFlatTable): Unit = {
    if (streamFlatTable.shouldRefreshTable && tableRefreshAcc.get > streamFlatTable.tableRefreshInterval) {
      log.info("refresh dimension tables.")
      streamFlatTable.lookupTablesGlobal.foreach { case (_, df) =>
        df.unpersist(true)
      }
      streamFlatTable.loadLookupTables()
      tableRefreshAcc.set(0)
    }
  }

  def registerStreamListener(): Unit = {
    def getTime(time: String): Long = {
      // Spark official time format
      // @param timestamp Beginning time of the trigger in ISO8601 format, i.e. UTC timestamps.
      // "timestamp" : "2016-12-14T18:45:24.873Z"
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.getDefault(Locale.Category.FORMAT))
      format.setTimeZone(TimeZone.getTimeZone("UTC"))
      format.parse(time).getTime
    }

    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {

      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {

      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        val progress = queryProgress.progress
        val batchRows = progress.numInputRows
        val time = getTime(progress.timestamp)
        val now = System.currentTimeMillis()
        var minDataLatency = 0L
        var maxDataLatency = 0L
        if (minMaxBuffer.nonEmpty) {
          minDataLatency = (now - minMaxBuffer(0)._1) + durationSec * 1000
          maxDataLatency = (now - minMaxBuffer(0)._2) + durationSec * 1000
          minMaxBuffer.clear()
        }
        val durationMs = progress.durationMs.get("triggerExecution").longValue()
        val request = new StreamingJobStatsRequest(jobId, project, batchRows, batchRows / durationSec, durationMs,
          time, minDataLatency, maxDataLatency)
        val rest = createRestSupport(kylinConfig)
        try {
          request.setJobExecutionId(jobExecId)
          request.setJobType(jobType.name())
          rest.execute(rest.createHttpPut("/streaming_jobs/stats"), request)
        } catch {
          case e: Exception => logError("Streaming Stats Rest Request Failed...", e)
        } finally {
          rest.close()
        }
      }
    })
  }

  def addShutdownListener(): Unit = {
    ss.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {

      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        StreamingUtils.replayAuditlog()
        if (isGracefulShutdown(project, jobId)) {
          log.info("onQueryProgress begin to shutdown streaming build job (" + event + ")")
          setStopFlag(true)
        }
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        log.info("onQueryTerminated begin to shutdown streaming build job (" + event + ")")
        JobKiller.killApplication(jobId)
      }
    })
  }


  override def getStopFlag: Boolean = {
    gracefulStop.get()
  }

  override def setStopFlag(stopFlag: Boolean): Unit = {
    gracefulStop.set(stopFlag)
  }
}


