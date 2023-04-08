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

package org.apache.spark.sql.execution.datasource

import java.sql.{Date, Timestamp}
import java.util

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.exception.TargetSegmentNotFoundException
import org.apache.kylin.common.util.{DateFormat, HadoopUtil}
import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.engine.spark.filter.QueryFiltersCollector.increaseHit
import org.apache.kylin.engine.spark.utils.{LogEx, LogUtils}
import org.apache.kylin.guava30.shaded.common.collect.Sets
import org.apache.kylin.metadata.cube.model.{DimensionRangeInfo, LayoutEntity, NDataflow, NDataflowManager}
import org.apache.kylin.metadata.datatype.DataType
import org.apache.kylin.metadata.model.{PartitionDesc, TblColRef}
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConverters._
import scala.collection.mutable

case class SegmentDirectory(segmentID: String, partitions: List[Long], files: Seq[FileStatus])

/**
 * A container for shard information.
 * Sharding is a technology for decomposing data sets into more manageable parts, and the number
 * of shards is fixed so it does not fluctuate with data.
 *
 * @param numShards        number of shards.
 * @param shardColumnNames the names of the columns that used to generate the shard id.
 * @param sortColumnNames  the names of the columns that used to sort data in each shard.
 */
case class ShardSpec(numShards: Int,
                     shardColumnNames: Seq[String],
                     sortColumnNames: Seq[String]) {

  if (numShards <= 0) {
    throw new AnalysisException(
      s"Number of shards should be greater than 0.")
  }

  override def toString: String = {
    val str = s"shard column: [${shardColumnNames.mkString(", ")}]"
    val sortString = if (sortColumnNames.nonEmpty) {
      s", sort columns: [${sortColumnNames.mkString(", ")}]"
    } else {
      ""
    }
    s"$numShards shards, $str$sortString"
  }
}

class FilePruner(val session: SparkSession,
                 val options: Map[String, String],
                 val dataSchema: StructType)
  extends FileIndex with ResetShufflePartition with LogEx {

  private val dataflow: NDataflow = {
    val dataflowId = options.getOrElse("dataflowId", sys.error("dataflowId option is required"))
    val prj = options.getOrElse("project", sys.error("project option is required"))
    val prunedSegmentIds = Sets.newHashSet(prunedSegmentDirs.map(_.segmentID).asJavaCollection)
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, prj)
    // init pruned Segment LayoutInfo immediately
    val dataflow = dfMgr.getDataflow(dataflowId, prunedSegmentIds)
    FilePruner.checkSegmentStatus(prunedSegmentIds, dataflow)
    dataflow
  }

  private val layout: LayoutEntity = {
    val cuboidId = options.getOrElse("cuboidId", sys.error("cuboidId option is required")).toLong
    dataflow.getIndexPlan.getLayoutEntity(cuboidId)
  }

  val workingDir: String = KapConfig.wrap(dataflow.getConfig).getReadParquetStoragePath(dataflow.getProject)
  val isFastBitmapEnabled: Boolean = options.apply("isFastBitmapEnabled").toBoolean

  override def rootPaths: Seq[Path] = {
    dataflow.getQueryableSegments.asScala.map(
      seg => new Path(toPath(seg.getId))
    )
  }

  def toPath(segmentId: String): String = {
    if (isFastBitmapEnabled) {
      s"$workingDir${dataflow.getUuid}/${segmentId}/${layout.getId}${HadoopUtil.FAST_BITMAP_SUFFIX}"
    } else {
      s"$workingDir${dataflow.getUuid}/${segmentId}/${layout.getId}"
    }
  }

  private lazy val prunedSegmentDirs: Seq[SegmentDirectory] = {
    val prunedSegmentInfo = options.getOrElse("pruningInfo", sys.error("pruningInfo option is required")).split(",")
    prunedSegmentInfo.map(segInfo => {
      if (segInfo.contains(":")) {
        val segmentPartitions = segInfo.split(":")
        SegmentDirectory(segmentPartitions(0), segmentPartitions(1).split("\\|").map(id => id.toLong).toList, null)
      } else {
        SegmentDirectory(segInfo, List.empty[Long], null)
      }
    })
  }

  override lazy val partitionSchema: StructType = {
    // we did not use the partitionBy mechanism of spark
    new StructType()
  }

  var pattern: String = _

  lazy val timePartitionSchema: StructType = {
    val desc: PartitionDesc = dataflow.getModel.getPartitionDesc
    StructType(
      if (desc != null) {
        val ref = desc.getPartitionDateColumnRef
        // only consider partition date column
        // we can only get col ID in layout cuz data schema is all ids.
        val id = layout.getOrderedDimensions.inverse().get(ref)
        if (id != null && (ref.getType.isDateTimeFamily || ref.getType.isStringFamily)) {
          if (ref.getType.isStringFamily || ref.getType.isDateTimeFamily) {
            pattern = desc.getPartitionDateFormat
          }
          dataSchema.filter(_.name == id.toString)

        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      })
  }

  lazy val shardBySchema: StructType = {
    val shardByCols = layout.getShardByColumns.asScala.map(_.toString)

    StructType(
      if (shardByCols.isEmpty) {
        Seq.empty
      } else {
        dataSchema.filter(f => shardByCols.contains(f.name))
      })
  }

  // timePartitionColumn is the mechanism of kylin.
  private var timePartitionColumn: Attribute = _

  private var shardByColumn: Attribute = _

  private var isResolved: Boolean = false

  def resolve(relation: LogicalRelation, resolver: Resolver): Unit = {
    val timePartitionAttr = relation.resolve(timePartitionSchema, resolver)
    if (timePartitionAttr.nonEmpty) {
      timePartitionColumn = timePartitionAttr.head
    }

    val shardByAttr = relation.resolve(shardBySchema, resolver)
    if (shardByAttr.nonEmpty) {
      shardByColumn = shardByAttr.head
    }
    isResolved = true
  }

  def getShardSpec: Option[ShardSpec] = {
    if (!options.getOrElse("bucketingEnabled", "false").toBoolean) {
      return None
    }

    val segIds = options.getOrElse("pruningInfo", throw new RuntimeException("empty pruningInfo")).split(',').map(segInfo =>
      segInfo.split(":")(0)
    )
    val segs = dataflow.getQueryableSegments.asScala.filter(seg => segIds.contains(seg.getId))
    assert(segs.nonEmpty, "No queryable segments")
    val shardNum = segs.head.getLayout(layout.getId).getPartitionNum

    if (layout.getShardByColumns.isEmpty ||
      segs.exists(_.getLayout(layout.getId).getPartitionNum != shardNum)) {
      logInfo("Shard by column is empty or segments have a different number of shards, skip shard join opt.")
      None
    } else {
      val sortColumns = if (segs.length == 1) {
        layout.getOrderedDimensions.keySet.asScala.map(_.toString).toSeq
      } else {
        logInfo("Sort order will lost in multi segments.")
        Seq.empty
      }

      Some(ShardSpec(shardNum, shardBySchema.fieldNames.toSeq, sortColumns))
    }
  }

  var cached = new java.util.HashMap[(Seq[Expression], Seq[Expression], Seq[Expression]), (Seq[PartitionDirectory], Long)]()

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    listFiles(partitionFilters, dataFilters, Seq.empty[Expression]);
  }

  def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression],
                derivedFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (cached.containsKey((partitionFilters, dataFilters, derivedFilters))) {
      return cached.get((partitionFilters, dataFilters, derivedFilters))._1
    }

    require(isResolved)
    val timePartitionFilters = getSpecFilter(dataFilters, timePartitionColumn)
    val dimFilters = getDimFilter(dataFilters, timePartitionColumn, shardByColumn)
    val derivedDimFilters = getDimFilter(derivedFilters, timePartitionColumn, shardByColumn)
    logInfoIf(timePartitionFilters.nonEmpty)(s"Applying time partition filters: ${timePartitionFilters.mkString(",")}")

    // segment pruning
    val project = dataflow.getProject
    val projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv).getProject(project).getConfig

    var selected = prunedSegmentDirs;
    if (projectKylinConfig.isSkipEmptySegments) {
      selected = afterPruning("pruning empty segment", null, selected) {
        (_, segDirs) => pruneEmptySegments(segDirs)
      }
    }

    selected = afterPruning("pruning segment with time partition", timePartitionFilters, selected) {
      pruneSegments
    }
    val filteredSizeAfterTimePartition = selected.size
    var filteredSizeAfterDimensionFilter = selected.size

    if (projectKylinConfig.isDimensionRangeFilterEnabled) {
      selected = afterPruning("pruning segment with dimension range", dimFilters, selected) {
        pruneSegmentsDimRange
      }
      filteredSizeAfterDimensionFilter = selected.size
      selected = afterPruning("pruning segment with derived dimension range", derivedDimFilters, selected) {
        pruneSegmentsDimRange
      }
    }

    QueryContext.current().record("seg_pruning")
    QueryContext.current().getMetrics.setSegCount(selected.size)

    logInfo(s"Segment Num: Before filter: ${prunedSegmentDirs.size}, After time partition filter: " +
      s"$filteredSizeAfterTimePartition, After dimension filter: ${filteredSizeAfterDimensionFilter}, " +
      s"After derived dimension filter: ${selected.size}.")
    selected = selected.par.map { e =>
      val logString = s"[fetch file status for Segment ID: ${e.segmentID}; Partition Num: ${e.partitions.size}]"
      logTime(logString, true) {
        var statuses = Seq.empty[FileStatus]
        e.partitions.foreach(id => {
          val bucketId = dataflow.getSegment(e.segmentID).getBucketId(layout.getId, id)
          val childDir = if (bucketId == null) id else bucketId
          val path = new Path(toPath(e.segmentID) + s"/${childDir}")
          statuses = statuses ++ getFileStatues(e.segmentID, path)
        })
        if (statuses.isEmpty) {
          statuses = statuses ++ getFileStatues(e.segmentID, new Path(toPath(e.segmentID)))
        }

        SegmentDirectory(e.segmentID, e.partitions, statuses)
      }
    }.toIterator.toSeq
    QueryContext.current().record("fetch_file_status")
    // shards pruning
    selected = afterPruning("pruning shard", dataFilters, selected) {
      pruneShards
    }
    QueryContext.current().record("shard_pruning")
    val totalFileCount = selected.flatMap(partition => partition.files).size
    QueryContext.current().getMetrics.setFileCount(totalFileCount)
    val totalFileSize = selected.flatMap(partition => partition.files).map(_.getLen).sum
    val sourceRows = selected.map(seg => {
      val segment = dataflow.getSegment(seg.segmentID)
      val dataLayout = segment.getLayout(layout.getId)
      val layoutRows = if (dataLayout == null) 0 else dataLayout.getRows
      logInfo(s"Source scan rows: Query Id: ${QueryContext.current().getQueryId}, Segment Id: ${seg.segmentID}, " +
        s"Layout Id: ${layout.getId}, rows: $layoutRows.")
      layoutRows
    }).sum
    setShufflePartitions(totalFileSize, sourceRows, session)
    if (selected.isEmpty) {
      val value = Seq.empty[PartitionDirectory]
      cached.put((partitionFilters, dataFilters, derivedFilters), (value, sourceRows))
      value
    } else {
      val value = Seq(PartitionDirectory(InternalRow.empty, selected.flatMap(_.files)))
      cached.put((partitionFilters, dataFilters, derivedFilters), (value, sourceRows))
      value
    }

  }

  private def getFileStatues(segmentId: String, path: Path): Seq[FileStatus] = {
    val fsc = ShardFileStatusCache.getFileStatusCache(session)

    val maybeStatuses = fsc.getLeafFiles(path)
    val lastBuildTime = dataflow.getSegment(segmentId).getLastBuildTime
    var cacheValid = false
    if (maybeStatuses.isDefined) {
      cacheValid = lastBuildTime.equals(ShardFileStatusCache.getSegmentBuildTime(segmentId))
    }

    if (cacheValid) {
      maybeStatuses.get
    } else {
      val statuses = path.getFileSystem(session.sparkContext.hadoopConfiguration).listStatus(path)
      fsc.putLeafFiles(path, statuses)
      ShardFileStatusCache.refreshSegmentBuildTimeCache(segmentId, lastBuildTime)
      statuses
    }
  }

  private def afterPruning(pruningType: String, specFilters: Seq[Expression], inputs: Seq[SegmentDirectory])
                          (pruningFunc: (Seq[Expression], Seq[SegmentDirectory]) => Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    if (specFilters != null && specFilters.isEmpty) {
      inputs
    } else {
      var selected = inputs
      try {
        logTime(pruningType, debug = true) {
          selected = pruningFunc(specFilters, inputs)
          logInfo(s"$pruningType: ${FilePruner.prunedSegmentInfo(inputs, selected)}")
        }
      } catch {
        case th: Throwable =>
          logWarning(s"Error occurs when $pruningType, scan all ${pruningType}s.", th)
      }
      selected
    }
  }

  private def getSpecFilter(dataFilters: Seq[Expression], col: Attribute): Seq[Expression] = {
    if (col != null) {
      dataFilters.filter(_.references.subsetOf(AttributeSet(col)))
    } else Seq.empty
  }

  private def getDimFilter(dataFilters: Seq[Expression], timeCol: Attribute, shardCol: Attribute): Seq[Expression] = {
    dataFilters.filterNot(_.references.subsetOf(AttributeSet(Seq(timeCol, shardCol).filter(_ != null))))
  }

  private def pruneEmptySegments(segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    segDirs.filter(seg => {
      val segment = dataflow.getSegment(seg.segmentID)
      val dataLayout = segment.getLayout(layout.getId)
      if (dataLayout == null || dataLayout.isEmpty) {
        logDebug(s"pruning empty segment: segment ${seg.segmentID} ${layout.getId} is empty.")
        false
      } else {
        true
      }
    })
  }

  private def pruneSegments(filters: Seq[Expression],
                            segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {

    val filteredStatuses = if (filters.isEmpty) {
      segDirs
    } else {
      val reducedFilter = filters.toList.map(filter => convertCastFilter(filter))
        .flatMap(f => DataSourceStrategy.translateFilter(f, true)).reduceLeft(And)
      segDirs.filter {
        e => {
          if (dataflow.getSegment(e.segmentID).isOffsetCube) {
            val ksRange = dataflow.getSegment(e.segmentID).getKSRange
            SegFilters(ksRange.getStart, ksRange.getEnd, pattern).foldStreamingFilter(reducedFilter) match {
              case Trivial(true) => true
              case Trivial(false) => false
            }
          } else {
            val tsRange = dataflow.getSegment(e.segmentID).getTSRange
            val start = DateFormat.getFormatTimeStamp(tsRange.getStart.toString, pattern)
            val end = DateFormat.getFormatTimeStamp(tsRange.getEnd.toString, pattern)
            SegFilters(start, end, pattern).foldFilter(reducedFilter) match {
              case Trivial(true) => true
              case Trivial(false) => false
            }
          }
        }
      }
    }
    filteredStatuses
  }

  private def pruneSegmentsDimRange(filters: Seq[Expression],
                                    segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    val hitColumns = Sets.newHashSet[String]()
    val project = options.getOrElse("project", "")
    val filteredStatuses = if (filters.isEmpty) {
      segDirs
    } else {
      val reducedFilter = filters.toList.map(filter => convertCastFilter(filter))
        .flatMap(f => DataSourceStrategy.translateFilter(f, true)).reduceLeft(And)

      segDirs.filter {
        e => {
          val dimRange = dataflow.getSegment(e.segmentID).getDimensionRangeInfoMap
          if (dimRange != null && !dimRange.isEmpty) {
            SegDimFilters(dimRange, dataflow.getIndexPlan.getEffectiveDimCols, dataflow.getId, project, hitColumns)
              .foldFilter(reducedFilter) match {
              case Trivial(true) => true
              case Trivial(false) => false
            }
          } else {
            true
          }
        }
      }
    }
    hitColumns.forEach(col => increaseHit(project, dataflow.getId, col))
    filteredStatuses
  }

  private def pruneShards(filters: Seq[Expression],
                          segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    val filteredStatuses = if (layout.getShardByColumns.size() != 1) {
      segDirs
    } else {
      val normalizedFiltersAndExpr = filters.reduce(expressions.And)

      val pruned = segDirs.map { case SegmentDirectory(segID, partitions, files) =>
        val partitionNumber = dataflow.getSegment(segID).getLayout(layout.getId).getPartitionNum
        require(partitionNumber > 0, "Shards num with shard by col should greater than 0.")

        val bitSet = getExpressionShards(normalizedFiltersAndExpr, shardByColumn.name, partitionNumber)

        val selected = files.filter(f => {
          val partitionId = FilePruner.getPartitionId(f.getPath)
          bitSet.get(partitionId)
        })
        SegmentDirectory(segID, partitions, selected)
      }
      pruned
    }
    filteredStatuses
  }

  override lazy val inputFiles: Array[String] = Array.empty[String]

  override lazy val sizeInBytes: Long = {
    dataflow.getQueryableSegments.asScala.map(seg => seg.getLayout(layout.getId))
      .filter(_ != null).map(_.getByteSize).sum
  }

  override def refresh(): Unit = {}

  private def getExpressionShards(expr: Expression,
                                  shardColumnName: String,
                                  numShards: Int): BitSet = {

    def getShardNumber(attr: Attribute, v: Any): Int = {
      BucketingUtils.getBucketIdFromValue(attr, numShards, v)
    }

    def getShardSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
      val matchedShards = new BitSet(numShards)
      val prj = options.getOrElse("project", sys.error("project option is required"))
      val skipShardPruning = NProjectManager.getProjectConfig(prj).skipShardPruningForInExpr && iter.size > 256
      if (skipShardPruning) {
        matchedShards.setUntil(matchedShards.capacity)
      } else {
        iter.map(v => getShardNumber(attr, v))
          .foreach(shardNum => matchedShards.set(shardNum))
      }
      matchedShards
    }

    def getShardSetFromValue(attr: Attribute, v: Any): BitSet = {
      val matchedShards = new BitSet(numShards)
      matchedShards.set(getShardNumber(attr, v))
      matchedShards
    }

    expr match {
      case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == shardColumnName =>
        getShardSetFromValue(a, v)
      case expressions.In(a: Attribute, list)
        if list.forall(_.isInstanceOf[Literal]) && a.name == shardColumnName =>
        getShardSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
      case expressions.InSet(a: Attribute, hset) if a.name == shardColumnName =>
        getShardSetFromIterable(a, hset)
      case expressions.IsNull(a: Attribute) if a.name == shardColumnName =>
        getShardSetFromValue(a, null)
      case expressions.And(left, right) =>
        getExpressionShards(left, shardColumnName, numShards) &
          getExpressionShards(right, shardColumnName, numShards)
      case expressions.Or(left, right) =>
        getExpressionShards(left, shardColumnName, numShards) |
          getExpressionShards(right, shardColumnName, numShards)
      case _ =>
        val matchedShards = new BitSet(numShards)
        matchedShards.setUntil(numShards)
        matchedShards
    }
  }

  //  translate for filter type match
  private def convertCastFilter(filter: Expression): Expression = {
    filter match {
      case expressions.EqualTo(expressions.Cast(a: Attribute, _, _, _), Literal(v, t)) =>
        expressions.EqualTo(a, Literal(v, t))
      case expressions.EqualTo(Literal(v, t), expressions.Cast(a: Attribute, _, _, _)) =>
        expressions.EqualTo(Literal(v, t), a)
      case expressions.GreaterThan(expressions.Cast(a: Attribute, _, _, _), Literal(v, t)) =>
        expressions.GreaterThan(a, Literal(v, t))
      case expressions.GreaterThan(Literal(v, t), expressions.Cast(a: Attribute, _, _, _)) =>
        expressions.GreaterThan(Literal(v, t), a)
      case expressions.LessThan(expressions.Cast(a: Attribute, _, _, _), Literal(v, t)) =>
        expressions.LessThan(a, Literal(v, t))
      case expressions.LessThan(Literal(v, t), expressions.Cast(a: Attribute, _, _, _)) =>
        expressions.LessThan(Literal(v, t), a)
      case expressions.GreaterThanOrEqual(expressions.Cast(a: Attribute, _, _, _), Literal(v, t)) =>
        expressions.GreaterThanOrEqual(a, Literal(v, t))
      case expressions.GreaterThanOrEqual(Literal(v, t), expressions.Cast(a: Attribute, _, _, _)) =>
        expressions.GreaterThanOrEqual(Literal(v, t), a)
      case expressions.LessThanOrEqual(expressions.Cast(a: Attribute, _, _, _), Literal(v, t)) =>
        expressions.LessThanOrEqual(a, Literal(v, t))
      case expressions.LessThanOrEqual(Literal(v, t), expressions.Cast(a: Attribute, _, _, _)) =>
        expressions.LessThanOrEqual(Literal(v, t), a)
      case expressions.Or(left, right) =>
        expressions.Or(convertCastFilter(left), convertCastFilter(right))
      case expressions.And(left, right) =>
        expressions.And(convertCastFilter(left), convertCastFilter(right))
      case expressions.Not(child) =>
        expressions.Not(convertCastFilter(child))
      case _ => filter
    }
  }
}

object FilePruner {
  def getPartitionId(p: Path): Int = {
    // path like: part-00001-91f13932-3d5e-4f85-9a56-d1e2b47d0ccb-c000.snappy.parquet
    // we need to get 00001.
    val partitionId = p.getName.split("-", 3)(1).toInt
    partitionId
  }

  def prunedSegmentInfo(segDirs: Seq[SegmentDirectory], prunedDirs: Seq[SegmentDirectory]): String = {
    val files: Seq[SegmentDirectory] => Seq[Long] =
      _.flatMap(s => if (s.files == null) Nil else s.files).map(_.getLen)

    val all = files(segDirs)
    val pruned = files(prunedDirs)

    val summary =
      s""""nums":"${prunedDirs.size}/${segDirs.size}","files":"${pruned.size}/${all.size}",
         |"bytes":"${pruned.sum}/${all.sum}"""".stripMargin.replaceAll("\\n", " ")

    if (prunedDirs.nonEmpty && prunedDirs.size < segDirs.size) {
      val prunedDetails = LogUtils.jsonArray(prunedDirs)(_.segmentID)
      val detail = s""""pruned":$prunedDetails"""
      s"{$summary,$detail}"
    } else {
      s"{$summary}"
    }
  }

  def checkSegmentStatus(prunedSegmentIds: util.HashSet[String], dataflow: NDataflow): Unit = {
    // check whether each segment id corresponds to the segment in NDataflow
    val filterSegmentIds = dataflow.getSegments(prunedSegmentIds).asScala.map(e => e.getId).toSet
    if (prunedSegmentIds.size != filterSegmentIds.size) {
      val missSegId = new mutable.StringBuilder
      prunedSegmentIds.asScala.foreach(e => {
        if (!filterSegmentIds.contains(e)) {
          missSegId.append(e).append(";")
        }
      })
      throw new TargetSegmentNotFoundException(missSegId.toString)
    }
  }
}

case class SegFilters(start: Long, end: Long, pattern: String) extends Logging {

  private def insurance(value: Any)
                       (func: Long => Filter): Filter = {
    value match {
      case v: Date =>
        // see SPARK-27546
        val ts = DateFormat.stringToMillis(v.toString)
        func(ts)
      case v: String if pattern != null =>
        val format = DateFormat.getDateFormat(pattern)
        val time = format.parse(v.toString).getTime
        func(time)
      case v: Timestamp =>
        func(v.getTime)
      case _ =>
        Trivial(true)
    }
  }

  /**
   * Recursively fold provided filters to trivial,
   * blocks are always non-empty.
   */
  def foldFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start && ts < end)
        }
      case In(_, values: Array[Any]) =>
        val satisfied = values.map(v => insurance(v) {
          ts => Trivial(ts >= start && ts < end)
        }).exists(_.equals(Trivial(true)))
        Trivial(satisfied)

      case IsNull(_) =>
        Trivial(false)
      case IsNotNull(_) =>
        Trivial(true)
      case GreaterThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts < end)
        }
      case GreaterThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts < end)
        }
      case LessThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts > start)
        }
      case LessThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start)
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }

  def foldStreamingFilter(filter: Filter): Filter = {
    filter match {
      case EqualTo(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start && ts <= end)
        }
      case In(_, values: Array[Any]) =>
        val satisfied = values.map(v => insurance(v) {
          ts => Trivial(ts >= start && ts <= end)
        }).exists(_.equals(Trivial(true)))
        Trivial(satisfied)

      case IsNull(_) =>
        Trivial(false)
      case IsNotNull(_) =>
        Trivial(true)
      case GreaterThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts < end)
        }
      case GreaterThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts <= end)
        }
      case LessThan(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts > start)
        }
      case LessThanOrEqual(_, value: Any) =>
        insurance(value) {
          ts => Trivial(ts >= start)
        }
      case And(left: Filter, right: Filter) =>
        And(foldStreamingFilter(left), foldStreamingFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldStreamingFilter(left), foldStreamingFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case unsupportedFilter =>
        // return 'true' to scan all partitions
        // currently unsupported filters are:
        // - StringStartsWith
        // - StringEndsWith
        // - StringContains
        // - EqualNullSafe
        Trivial(true)
    }
  }
}


abstract class PushableColumnBase {

  def unapply(col: String): String = {
    s"${col.replace("`", "")}"
  }

}

case class SegDimFilters(dimRange: java.util.Map[String, DimensionRangeInfo], dimCols: java.util.Map[Integer, TblColRef],
                         dataflowId: String, project: String, hitColumns: java.util.Set[String]) extends Logging {

  private def insurance(id: String, value: Any)
                       (func: Any => Filter): Filter = {
    if (dimRange.containsKey(id) && dimCols.containsKey(id.toInt)) {
      func(value)
    } else {
      Trivial(true)
    }
  }

  /**
   * Recursively fold provided filters to trivial,
   * blocks are always non-empty.
   */

  def escapeQuote(colName: String): String = {
    s"${colName.replace("`", "")}"
  }

  def foldFilter(filter: Filter): Filter = {

    def getDataType(col: String, value: Any): DataType = {
      if (value.isInstanceOf[Date] || value.isInstanceOf[Timestamp]) return DataType.getType("date")
      dimCols.get(col.toInt).getType
    }

    filter match {
      case EqualTo(id, value: Any) =>
        val col = escapeQuote(id)
        hitColumns.add(col)
        insurance(col, value) {
          ts => {
            val dataType = getDataType(col, value)
            Trivial(dataType.compare(ts.toString, dimRange.get(col).getMin) >= 0
              && dataType.compare(ts.toString, dimRange.get(col).getMax) <= 0)
          }
        }
      case In(id, values: Array[Any]) =>
        val col = escapeQuote(id)
        hitColumns.add(col)
        val satisfied = values.map(v => insurance(col, v) {
          ts => {
            val dataType = getDataType(col, v)
            Trivial(dataType.compare(ts.toString, dimRange.get(col).getMin) >= 0
              && dataType.compare(ts.toString, dimRange.get(col).getMax) <= 0)
          }
        }).exists(_.equals(Trivial(true)))
        Trivial(satisfied)

      case IsNull(_) =>
        Trivial(true)
      case IsNotNull(_) =>
        Trivial(true)
      case GreaterThan(id, value: Any) =>
        val col = escapeQuote(id)
        insurance(col, value) {
          ts => Trivial(getDataType(col, value).compare(ts.toString, dimRange.get(col).getMax) < 0)
        }
      case GreaterThanOrEqual(id, value: Any) =>
        val col = escapeQuote(id)
        insurance(col, value) {
          ts => Trivial(getDataType(col, value).compare(ts.toString, dimRange.get(col).getMax) <= 0)
        }
      case LessThan(id, value: Any) =>
        val col = escapeQuote(id)
        insurance(col, value) {
          ts => Trivial(getDataType(col, value).compare(ts.toString, dimRange.get(col).getMin) > 0)
        }
      case LessThanOrEqual(id, value: Any) =>
        val col = escapeQuote(id)
        insurance(col, value) {
          ts => Trivial(getDataType(col, value).compare(ts.toString, dimRange.get(col).getMin) >= 0)
        }
      case And(left: Filter, right: Filter) =>
        And(foldFilter(left), foldFilter(right)) match {
          case And(Trivial(false), _) => Trivial(false)
          case And(_, Trivial(false)) => Trivial(false)
          case And(Trivial(true), right) => right
          case And(left, Trivial(true)) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(Trivial(true), _) => Trivial(true)
          case Or(_, Trivial(true)) => Trivial(true)
          case Or(Trivial(false), right) => right
          case Or(left, Trivial(false)) => left
          case other => other
        }
      case trivial: Trivial =>
        trivial
      case _ =>
        Trivial(true)
    }
  }
}
