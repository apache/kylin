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

package org.apache.kylin.engine.spark.job.stage.build

import com.google.common.collect.Queues
import org.apache.commons.math3.ml.clustering.{Clusterable, KMeansPlusPlusClusterer}
import org.apache.commons.math3.ml.distance.EarthMoversDistance
import org.apache.kylin.common.persistence.transaction.UnitOfWork
import org.apache.kylin.common.persistence.transaction.UnitOfWork.Callback
import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.builder.DictionaryBuilderHelper
import org.apache.kylin.engine.spark.job._
import org.apache.kylin.engine.spark.job.stage.build.FlatTableAndDictBase.Statistics
import org.apache.kylin.engine.spark.job.stage.{BuildParam, InferiorGroup, StageExec}
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree.TreeNode
import org.apache.kylin.metadata.cube.model.NIndexPlanManager.NIndexPlanUpdater
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel

import java.util.Objects
import java.util.concurrent.CountDownLatch
import scala.collection.JavaConverters._
import scala.collection.mutable


abstract class BuildStage(private val jobContext: SegmentJob,
                          private val dataSegment: NDataSegment,
                          private val buildParam: BuildParam)
  extends SegmentExec with StageExec {
  override def getJobContext: SparkApplication = jobContext

  override def getDataSegment: NDataSegment = dataSegment

  override def getSegmentId: String = dataSegment.getId

  // Needed variables from job context.
  protected final val jobId = jobContext.getJobId
  protected final val config = jobContext.getConfig
  protected final val dataflowId = jobContext.getDataflowId
  protected final val sparkSession = jobContext.getSparkSession
  protected final val resourceContext = jobContext.getBuildContext
  protected final val runtime = jobContext.getRuntime
  protected final val readOnlyLayouts = jobContext.getReadOnlyLayouts

  // Needed variables from data segment.
  protected final val segmentId = dataSegment.getId
  protected final val project = dataSegment.getProject

  protected final val dataModel = dataSegment.getModel
  protected final val storageType = dataModel.getStorageType

  private lazy val spanningTree = buildParam.getSpanningTree

  private lazy val flatTableDesc = buildParam.getFlatTableDesc

  private lazy val flatTable = buildParam.getBuildFlatTable

  private lazy val flatTableDS: Dataset[Row] = buildParam.getFlatTable
  private lazy val flatTableStats: Statistics = buildParam.getFlatTableStatistics

  private lazy val sanityResultQueue = Queues.newLinkedBlockingQueue[SanityResult]()

  // thread unsafe
  private lazy val cachedLayoutSanity = buildParam.getCachedLayoutSanity
  // thread unsafe
  private lazy val cachedLayoutDS = buildParam.getCachedLayoutDS
  // thread unsafe
  private lazy val cachedIndexInferior = buildParam.getCachedIndexInferior

  private lazy val datasetCacheStorageLevel = getStorageLevel

  protected val sparkSchedulerPool = "build"

  override protected def columnIdFunc(colRef: TblColRef): String = flatTableDesc.getColumnIdAsString(colRef)

  protected trait BuildTaskIterator[Task] extends Iterator[Task] {

    private var innerIter: Iterator[Task] = Iterator.empty

    def canSpan: Boolean

    def spanNodeSeq(segment: NDataSegment): Seq[TreeNode]

    def genTask(segment: NDataSegment, node: TreeNode): Seq[Task]

    override def hasNext: Boolean = {
      while ((Objects.isNull(innerIter) || !innerIter.hasNext) && canSpan) {
        innerIter = nextInnerIter
        // Avoid dead loop
        failFastCheck()
      }
      Objects.nonNull(innerIter) && innerIter.hasNext
    }

    override def next(): Task = {
      innerIter.next()
    }

    private def nextInnerIter: Iterator[Task] = {
      // Drain immediately.
      drain()
      // Use the latest data segment.
      val segment = jobContext.getSegment(segmentId)
      // span may generate empty nodes
      val nodes = spanNodeSeq(segment)
      nodes.flatMap(node => genTask(segment, node)).iterator
    }
  }

  protected def buildLayouts(): Unit = {

    val taskIter = new BuildTaskIterator[LayoutBuildTask] {

      override def canSpan: Boolean = spanningTree.canSpan

      override def spanNodeSeq(segment: NDataSegment): Seq[TreeNode] = {
        spanningTree.span(segment).asScala
      }

      override def genTask(segment: NDataSegment, node: TreeNode): Seq[LayoutBuildTask] = {
        getLayoutTasks(segment, node)
      }
    }
    slowStartExec(taskIter, buildLayout)
  }


  override protected def recordTaskInfo(t: Task): Unit = {
    logInfo(s"Segment $segmentId submit task: ${t.getTaskDesc}")
    KylinBuildEnv.get().buildJobInfos.recordCuboidsNumPerLayer(segmentId, 1)
  }

  override protected def reportTaskProgress(): Unit = {
    val layoutCount = KylinBuildEnv.get().buildJobInfos.getSeg2cuboidsNumPerLayer.get(segmentId).asScala.sum
    onBuildLayoutSuccess(layoutCount)
  }

  protected def buildStatistics(): Statistics = {
    flatTable.gatherStatistics()
  }

  protected def buildSanityCache(): Unit = {
    if (!config.isSanityCheckEnabled) {
      return
    }
    // Collect statistics for root nodes.
    val rootNodes = spanningTree.getRootNodes.asScala
    if (rootNodes.isEmpty) {
      return
    }

    val taskIter = rootNodes.map { node =>
      val layout = node.getLayout
      val layoutDS = getCachedLayoutDS(dataSegment, layout)
      new SanityTask(layout, layoutDS)
    }.iterator

    logInfo(s"Segment $segmentId build sanity cache.")
    slowStartExec(taskIter, (sanityTask: SanityTask) => {
      val layout = sanityTask.layout
      val layoutDS = sanityTask.layoutDS
      val sanityCount = SanityChecker.getCount(layoutDS, layout)
      sanityResultQueue.offer(new SanityResult(layout.getId, sanityCount))
    })

    val sanityMap = polledResultSeq(sanityResultQueue).map { r =>
      val layout = r.layout
      val sanityCount = r.sanityCount
      (layout, sanityCount)
    }.toMap.seq
    assert(sanityMap.keySet.size == rootNodes.size, //
      s"Collect sanity for root nodes went wrong: ${sanityMap.keySet.size} == ${rootNodes.size}")
    buildParam.setCachedLayoutSanity(Some(sanityMap))
    logInfo(s"Segment $segmentId finished build sanity cache $sanityMap.")
  }

  private def getLayoutTasks(segment: NDataSegment, node: TreeNode): Seq[LayoutBuildTask] = {
    val layouts = node.getLayouts.asScala // skip layouts
      .filterNot(layout => needSkipLayout(layout.getId, segment))
    if (layouts.isEmpty) {
      return Seq.empty
    }
    val columns = if (node.parentIsNull) {
      columnsFromFlatTable(node.getIndex)
    } else {
      columnsFromParentLayout(node.getIndex)
    }
    logInfo(s"Segment $segmentId index select columns " + //
      s"${node.getIndex.getId} ${columns.mkString("[", ",", "]")}")
    val sanityCount = getCachedLayoutSanity(node)
    if (node.parentIsNull) {
      // Build from flat table
      val inferior = getCachedIndexInferior(node.getIndex)
      // Parquet is a columnar storage format.
      // Non measured columns of flat table.
      val tableDS = if (inferior.isDefined) inferior.get.tableDS else flatTableDS
      val parentDS = if (columns.isEmpty) tableDS else tableDS.select(columns.map(col): _*)
      layouts.map { layout =>
        LayoutBuildTask(layout, None, parentDS, sanityCount, segment, inferior)
      }
    } else {
      // Build from data layout
      val parentLayout = node.getParent.getLayout
      val tableDS = getCachedLayoutDS(segment, parentLayout)
      val parentDS = if (columns.isEmpty) tableDS else tableDS.select(columns.map(col): _*)
      layouts.map { layout =>
        LayoutBuildTask(layout, Some(parentLayout), parentDS, sanityCount, segment)
      }
    }
  }

  private def getCachedLayoutSanity(node: TreeNode): Long = {
    // Not enabled.
    if (!config.isSanityCheckEnabled) {
      return SanityChecker.SKIP_FLAG
    }

    // From flat table.
    if (Objects.isNull(node.getRootNode)) {
      return flatTableStats.totalCount
    }

    cachedLayoutSanity.map { cached =>
      val layout = node.getRootNode.getLayout.getId
      assert(cached.contains(layout), //
        s"Root node's layout sanity should have been cached: layout")
      cached(layout)
    }.getOrElse(SanityChecker.SKIP_FLAG)
  }

  private def getCachedLayoutDS(segment: NDataSegment, layout: LayoutEntity): Dataset[Row] = synchronized {
    cachedLayoutDS.getOrElseUpdate(layout.getId,
      // Or update, lightweight invocation.
      {
        sparkSession.sparkContext.setJobDescription(s"Segment $segmentId prepare layout dataset ${layout.getId}")
        val layoutDS = StorageStoreUtils.toDF(segment, layout, sparkSession)
        sparkSession.sparkContext.setJobDescription(null)
        layoutDS
      })
  }

  // scala version < 2.13
  // which you could invoke with minOptionBy(seq)(_.something)
  private def minOptionBy[A, B: Ordering](seq: Seq[A])(f: A => B) =
    seq reduceOption Ordering.by(f).min

  sealed case class LayoutBuildTask(layout: LayoutEntity //
                                    , parentLayout: Option[LayoutEntity] //
                                    , parentDS: Dataset[Row] //
                                    , sanityCount: Long //
                                    , segment: NDataSegment = dataSegment //
                                    , inferior: Option[InferiorGroup] = None) extends Task {
    override def getTaskDesc: String = {
      s"layout ${layout.getId}"
    }
  }


  private def buildLayout(task: LayoutBuildTask): Unit = {
    // Cache if essential.
    tryCacheInferior(task.inferior)

    val layoutDS = wrapLayoutDS(task.layout, task.parentDS)
    val parentDesc = if (task.parentLayout.isEmpty) {
      if (task.inferior.isDefined) "inferior flat table" else "flat table"
    } else task.parentLayout.get.getId
    val readableDesc = s"Segment $segmentId build layout ${task.layout.getId} from $parentDesc"
    newDataLayout(task.segment, task.layout, layoutDS, readableDesc, Some(new SanityChecker(task.sanityCount)))

    // Mark sweep if essential.
    tryReapInferior(task.inferior)
  }

  private def tryCacheInferior(optInferior: Option[InferiorGroup]): Unit = {
    if (optInferior.isDefined && optInferior.get.notCached.get()) {
      val inferior = optInferior.get
      inferior.cacheLock.lockInterruptibly()
      try {
        if (inferior.notCached.get()) {
          logInfo(s"Segment $segmentId inferior persist ${inferior.tableDS.columns.mkString("[", ",", "]")}")
          sparkSession.sparkContext.setJobDescription(s"Segment $segmentId inferior persist ${inferior.tableDS.columns.length}")
          inferior.tableDS.persist(datasetCacheStorageLevel).count()
          inferior.notCached.set(false)
          sparkSession.sparkContext.setJobDescription(null)
        }
      } finally {
        inferior.cacheLock.unlock()
      }
    }
  }

  private def tryReapInferior(optInferior: Option[InferiorGroup]): Unit = {
    if (optInferior.isDefined) {
      val inferior = optInferior.get
      inferior.reapCount.countDown()
      if (inferior.reapCount.getCount > 0) {
        return
      }
      inferior.cacheLock.lockInterruptibly()
      try {
        if (!inferior.notCached.get()) {
          logInfo(s"Segment $segmentId inferior unpersist ${inferior.tableDS.columns.mkString("[", ",", "]")}")
          sparkSession.sparkContext.setJobDescription(s"Segment $segmentId inferior unpersist ${inferior.tableDS.columns.length}")
          inferior.tableDS.unpersist(blocking = true)
          inferior.notCached.set(true)
          sparkSession.sparkContext.setJobDescription(null)
        }
      } finally {
        inferior.cacheLock.unlock()
      }
    }
  }

  protected def tryRefreshColumnBytes(): Unit = {
    if (flatTableStats == null) {
      logInfo(s"Segment $segmentId skip refresh column bytes.")
      return
    }
    logInfo(s"Segment $segmentId refresh column bytes.")
    val stats = flatTableStats
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val dataflowManager = NDataflowManager.getInstance(config, project)
        val copiedDataflow = dataflowManager.getDataflow(dataflowId).copy()
        val copiedSegment = copiedDataflow.getSegment(segmentId)
        val dataflowUpdate = new NDataflowUpdate(dataflowId)
        copiedSegment.setSourceCount(stats.totalCount)
        // Cal segment dimension range
        if (!jobContext.isPartialBuild) {
          copiedSegment.setDimensionRangeInfoMap(
            calDimRange(dataSegment, flatTable.getFlatTableDS)
          )
        }
        // By design, no fencing.
        val columnBytes = copiedSegment.getColumnSourceBytes
        stats.columnBytes.foreach(kv => columnBytes.put(kv._1, kv._2))
        dataflowUpdate.setToUpdateSegs(copiedSegment)
        // The afterward step would dump the meta to hdfs-store.
        // We should only update the latest meta in mem-store.
        // Make sure the copied dataflow here is the latest.
        dataflowManager.updateDataflow(dataflowUpdate)
      }
    }, project)
  }

  private def needSkipLayout(layout: Long, segment: NDataSegment = dataSegment): Boolean = {
    // Check whether it is resumed or not.
    val dataLayout = segment.getLayout(layout)
    if (Objects.isNull(dataLayout)) {
      return false
    }
    logInfo(s"Segment $segmentId skip layout $layout.")
    true
  }

  protected def tryRefreshBucketMapping(): Unit = {
    val segment = jobContext.getSegment(segmentId)
    UnitOfWork.doInTransactionWithRetry(new Callback[Unit] {
      override def process(): Unit = {
        val indexPlan = segment.getIndexPlan
        val manager = NIndexPlanManager.getInstance(config, project)
        val mapping = indexPlan.getLayoutBucketNumMapping
        class UpdateBucketMapping extends NIndexPlanUpdater {
          override def modify(copied: IndexPlan): Unit = {
            copied.setLayoutBucketNumMapping(mapping)
          }
        }
        manager.updateIndexPlan(dataflowId, new UpdateBucketMapping)
      }
    }, project)
  }

  sealed class SanityTask(val layout: LayoutEntity, val layoutDS: Dataset[Row]) extends Task {
    override def getTaskDesc: String = {
      s"layout sanity ${layout.getId}"
    }
  }

  sealed class SanityResult(val layout: Long, val sanityCount: Long)

  // ----------------------------- Beta feature: Inferior Flat Table. ----------------------------- //

  private val cachedMeasureMap = dataModel.getEffectiveMeasures.asScala.map { case (id, measure) =>
    val bitmapCol = DictionaryBuilderHelper.needGlobalDict(measure)
    val columns = if (Objects.nonNull(bitmapCol)) {
      val id = dataModel.getColumnIdByColumnName(bitmapCol.getIdentity)
      Seq(s"${id}_KE_ENCODE")
    } else {
      Seq.empty[String]
    } ++
      measure.getFunction.getParameters.asScala.filter(_.isColumnType) //
        .map(p => s"${dataModel.getColumnIdByColumnName(p.getValue)}")
    (id, columns)
  }.toMap

  private def columnsFromFlatTable(index: IndexEntity): Seq[String] = {
    val columns = mutable.Set[String]()

    // Dimension columns
    index.getEffectiveDimCols.keySet().asScala.foreach(id => columns.add(s"$id"))

    // Measure referenced columns
    index.getEffectiveMeasures.keySet().asScala //
      .foreach { measureId =>
        cachedMeasureMap.getOrElse(measureId, Seq.empty[String]).foreach(columns.add)
      }

    columns.toSeq
  }

  private def columnsFromParentLayout(index: IndexEntity): Seq[String] = {
    val columns = mutable.Set[String]()

    // Dimension columns
    index.getEffectiveDimCols.keySet().asScala.foreach(id => columns.add(s"$id"))

    // Measures
    index.getEffectiveMeasures.keySet().asScala.foreach(id => columns.add(s"$id"))

    columns.toSeq
  }

  def getStorageLevel: StorageLevel = {
    config.getInferiorFlatTableStorageLevel match {
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "DISK_ONLY_2" => StorageLevel.DISK_ONLY_2
      case "DISK_ONLY_3" => StorageLevel.DISK_ONLY_3
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
      case "OFF_HEAP" => StorageLevel.OFF_HEAP
      case _ => StorageLevel.MEMORY_AND_DISK
    }
  }

  // Suitable for models generated from multi index recommendations.
  // TODO Make more fantastic abstractions.
  protected def buildInferior(): Unit = {

    val schema = flatTableDS.schema
    val arraySize = schema.size

    def getArray(index: IndexEntity): Array[Double] = {
      val arr = new Array[Double](arraySize)
      columnsFromFlatTable(index).foreach { column =>
        arr(schema.fieldIndex(column)) = 1.0d
      }
      arr
    }

    def getK(nodes: Seq[TreeNode]): Int = {
      val groupFactor = Math.max(1, config.getInferiorFlatTableGroupFactor)
      val k = Math.max(1, //
        Math.max(nodes.size / groupFactor //
          , nodes.map(_.getIndex) //
            .flatMap(_.getEffectiveDimCols.keySet().asScala) //
            .distinct.size / groupFactor))
      if (nodes.size < k) {
        1
      } else {
        k
      }
    }

    class ClusterNode(private val node: TreeNode) extends Clusterable {
      override def getPoint: Array[Double] = getArray(node.getIndex)

      def getNode: TreeNode = node
    }

    case class GroupedNodeColumn(nodes: Seq[TreeNode], columns: Seq[String])

    def cluster(nodes: Seq[TreeNode]): Seq[GroupedNodeColumn] = {
      if (nodes.isEmpty) {
        return Seq.empty[GroupedNodeColumn]
      }

      val k = getK(nodes)

      val kcluster = new KMeansPlusPlusClusterer[ClusterNode](k, -1, new EarthMoversDistance())
      kcluster.cluster( //
        scala.collection.JavaConverters.seqAsJavaList(nodes.map(n => new ClusterNode(n)))).asScala //
        .map { cluster =>
          val grouped = cluster.getPoints.asScala.map(_.getNode)
          val columns = grouped.map(_.getIndex).flatMap(columnsFromFlatTable).distinct.sorted
          GroupedNodeColumn(grouped, columns)
        }
    }

    val indexInferiorMap = mutable.HashMap[Long, InferiorGroup]()
    val dimensionFactor = Math.max(1, config.getInferiorFlatTableDimensionFactor)
    val nonSpanned = spanningTree.getFromFlatTableNodes.asScala.filter(_.nonSpanned())
    val clustered = nonSpanned.groupBy(node => node.getDimensionSize / dimensionFactor) //
      .values.filter { grouped =>
      val groupDesc = grouped.map(_.getIndex.getId).sorted.mkString("[", ",", "]")
      if (grouped.size > 1) {
        logInfo(s"Segment $segmentId coarse index group $groupDesc")
        true
      } else {
        logInfo(s"Segment $segmentId skip coarse index group $groupDesc")
        false
      }
    }.flatMap(cluster)

    val inferiors = clustered.zipWithIndex.map { case (grouped, i) =>
      logInfo(s"Segment $segmentId inferior indices columns " +
        s"${grouped.nodes.size} ${grouped.nodes.map(_.getIndex.getId).sorted.mkString("[", ",", "]")} " +
        s"${grouped.columns.size} ${grouped.columns.mkString("[", ",", "]")}")

      val reapCount = new CountDownLatch(grouped.nodes.map(node => node.getNonSpannedCount).sum)
      val tableDS = flatTableDS.select(grouped.columns.map(col): _*)

      val inferior = InferiorGroup(tableDS, reapCount)
      grouped.nodes.foreach { node =>
        indexInferiorMap.put(node.getIndex.getId, inferior)
        // Locality of reference principle.
        node.setLocalPriority(i)
      }
      inferior
    }

    if (inferiors.isEmpty) {
      return
    }

    buildParam.setCachedIndexInferior(Some(indexInferiorMap.toMap))
  }

  private def getCachedIndexInferior(index: IndexEntity): Option[InferiorGroup] = synchronized {
    if (cachedIndexInferior.isEmpty) {
      return None
    }
    val ic = cachedIndexInferior.get.getOrElse(index.getId, null)
    if (Objects.isNull(ic)) {
      return None
    }
    Some(ic)
  }

  // ----------------------------- Beta feature: Inferior Flat Table. ----------------------------- //

}
