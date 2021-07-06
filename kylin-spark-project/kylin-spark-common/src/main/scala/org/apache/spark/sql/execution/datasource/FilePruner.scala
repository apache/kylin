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
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.kylin.common.util.DateFormat
import org.apache.kylin.cube.cuboid.Cuboid
import org.apache.kylin.cube.CubeInstance
import org.apache.kylin.engine.spark.metadata.cube.PathManager
import org.apache.kylin.engine.spark.metadata.MetadataConverter
import org.apache.kylin.metadata.model.{PartitionDesc, SegmentStatusEnum}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EmptyRow, Expression, ExpressionUtils, Literal}
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConverters._

case class SegmentDirectory(segmentName: String, identifier: String, files: Seq[FileStatus])

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

class FilePruner(cubeInstance: CubeInstance,
                 cuboid: Cuboid,
                 val session: SparkSession,
                 val options: Map[String, String])
  extends FileIndex with ResetShufflePartition with Logging {

  val MAX_SHARDING_SIZE_PER_TASK: Long =
    cubeInstance.getConfig.getMaxShardingSizeMBPerTask * 1024 * 1024

  private lazy val segmentDirs: Seq[SegmentDirectory] = {
    cubeInstance.getSegments.asScala
      .filter(_.getStatus.equals(SegmentStatusEnum.READY)).map(seg => {
      SegmentDirectory(seg.getName, seg.getStorageLocationIdentifier, Nil)
    })
  }

  val layoutEntity = MetadataConverter.toLayoutEntity(cubeInstance, cuboid)

  val dataSchema: StructType = {
    StructType(layoutEntity.getOrderedDimensions.values().asScala
      .map { column => StructField(column.id.toString, column.dataType) }
      .toSeq ++
      layoutEntity.getOrderedMeasures.asScala
        .map { entry =>
          StructField(entry._1.toString, SparkTypeUtil.generateFunctionReturnDataType(entry._2)) }
        .toSeq)
  }

  override def rootPaths: Seq[Path] = {
    segmentDirs.map(seg => new Path(toPath(seg.segmentName, seg.identifier)))
  }

  def toPath(segmentName: String, identifier: String): String = {
    PathManager.getParquetStoragePath(cubeInstance, segmentName, identifier, cuboid.getId)
  }

  override lazy val partitionSchema: StructType = {
    // we did not use the partitionBy mechanism of spark
    new StructType()
  }

  var pattern: String = _

  lazy val timePartitionSchema: StructType = {
    val desc: PartitionDesc = cubeInstance.getModel.getPartitionDesc
    StructType(
      if (desc != null && desc.getPartitionDateColumnRef != null) {
        val ref = desc.getPartitionDateColumnRef
        // only consider partition date column
        // we can only get col ID in layout cuz data schema is all ids.
        val id = layoutEntity.getOrderedDimensions.asScala.values.find(
          column => column.columnName.equals(ref.getName))
        if (id.isDefined && (ref.getType.isDateTimeFamily || ref.getType.isStringFamily || ref.getType.isIntegerFamily)) {
          pattern = desc.getPartitionDateFormat
          dataSchema.filter(_.name == String.valueOf(id.get.id))
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      })
  }

  lazy val shardBySchema: StructType = {
    val shardByCols = layoutEntity.getShardByColumns.asScala.map(_.toString)

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

  private var shardSpec: Option[ShardSpec] = None

  def getShardSpec: Option[ShardSpec] = {
    shardSpec
  }

  private def genShardSpec(selected: Seq[SegmentDirectory]): Option[ShardSpec] = {
    if (!cubeInstance.getConfig.isShardingJoinOptEnabled || selected.isEmpty) {
      None
    } else {
      val segments = selected.par.map { segDir =>
        cubeInstance.getSegment(segDir.segmentName, SegmentStatusEnum.READY);
      }.seq
      val shardNum = segments.head.getCuboidShardNum(layoutEntity.getId).toInt

      // the shard num of all layout in segments must be the same
      if (layoutEntity.getShardByColumns.isEmpty || segments.exists(
        _.getCuboidShardNum(layoutEntity.getId).toInt != shardNum)) {
        logInfo("Shard by column is empty or segments have the different number of shard, " +
          "skip shard join.")
        None
      } else {
        // calculate the file size for each partition
        val partitionSizePerId = selected.flatMap(_.files).map( f =>
          (FilePruner.getPartitionId(f.getPath), f.getLen)
        ).groupBy(_._1).mapValues(_.map(_._2).sum)
        // if there are some partition ids which the file size exceeds the threshold
        if (partitionSizePerId.exists(_._2 > MAX_SHARDING_SIZE_PER_TASK)) {
          logInfo(s"There are some partition ids which the file size exceeds the " +
            s"threshold size ${MAX_SHARDING_SIZE_PER_TASK}, skip shard join.")
          None
        } else {
          val sortColumns = if (segments.length == 1) {
            layoutEntity.getOrderedDimensions.keySet().asScala.map(_.toString).toSeq
          } else {
            logInfo("Sort order will lost in multiple segments.")
            Seq.empty[String]
          }
          Some(ShardSpec(shardNum, shardBySchema.fieldNames.toSeq, sortColumns))
        }
      }
    }
  }

  var cached = new java.util.HashMap[(Seq[Expression], Seq[Expression]), Seq[PartitionDirectory]]()

  private def getFileStatusBySeg(seg: SegmentDirectory, fsc: FileStatusCache): SegmentDirectory = {
    val path = new Path(toPath(seg.segmentName, seg.identifier))
    val fs = path.getFileSystem(session.sparkContext.hadoopConfiguration)
    if (fs.isDirectory(path) && fs.exists(path)) {
      val maybeStatuses = fsc.getLeafFiles(path)
      if (maybeStatuses.isDefined) {
        SegmentDirectory(seg.segmentName, seg.identifier, maybeStatuses.get)
      } else {
        val statuses = fs.listStatus(path)
        fsc.putLeafFiles(path, statuses)
        SegmentDirectory(seg.segmentName, seg.identifier, statuses)
      }
    } else {
      logWarning(s"Segment path ${path.toString} not exists.")
      SegmentDirectory(seg.segmentName, seg.identifier, Nil)
    }
  }

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (cached.containsKey((partitionFilters, dataFilters))) {
      return cached.get((partitionFilters, dataFilters))
    }

    require(isResolved)
    val startTime = System.nanoTime
    val timePartitionFilters = getSegmentFilter(dataFilters, timePartitionColumn)
    logInfo(s"Applying time partition filters: ${timePartitionFilters.mkString(",")}")

    val fsc = ShardFileStatusCache.getFileStatusCache(session)

    // segment pruning
    var selected = afterPruning("segment", timePartitionFilters, segmentDirs) {
      pruneSegments
    }

    // fetch segment directories info in parallel
    selected = selected.par.map(seg => {
      getFileStatusBySeg(seg, fsc)
    }).filter(_.files.nonEmpty).seq

    // shards pruning
    selected = afterPruning("shard", dataFilters, selected) {
      pruneShards
    }
    // generate the ShardSpec
    shardSpec = genShardSpec(selected)
    //    QueryContextFacade.current().record("shard_pruning")
    val totalFileSize = selected.flatMap(_.files).map(_.getLen).sum
    logInfo(s"After files pruning, total file size is ${totalFileSize}")
    setShufflePartitions(totalFileSize, session, cubeInstance.getConfig)
    logInfo(s"Files pruning in ${(System.nanoTime() - startTime).toDouble / 1000000} ms")
    if (selected.isEmpty) {
      val value = Seq.empty[PartitionDirectory]
      cached.put((partitionFilters, dataFilters), value)
      value
    } else {
      val value = Seq(PartitionDirectory(InternalRow.empty, selected.flatMap(_.files)))
      cached.put((partitionFilters, dataFilters), value)
      value
    }
  }

  private def afterPruning(pruningType: String, specFilters: Seq[Expression],
                           inputs: Seq[SegmentDirectory])
                          (pruningFunc: (Seq[Expression], Seq[SegmentDirectory]) =>
                            Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    if (specFilters.isEmpty) {
      inputs
    } else {
      var selected = inputs
      try {
        val startTime = System.nanoTime
        selected = pruningFunc(specFilters, inputs)
        val endTime = System.nanoTime()
        logInfo(s"$pruningType pruning in ${(endTime - startTime).toDouble / 1000000} ms")
      } catch {
        case th: Throwable =>
          logError(s"Error occurs when $specFilters, scan all ${pruningType}s.", th)
      }
      selected
    }
  }

  private def getSegmentFilter(dataFilters: Seq[Expression], col: Attribute): Seq[Expression] = {
    dataFilters.map(extractSegmentFilter(_, col)).filter(_.isDefined).map(_.get)
  }

  private def extractSegmentFilter(filter: Expression, col: Attribute): Option[Expression] = {
    if (col == null) {
      return None
    }
    filter match {
      case expressions.Or(left, right) =>
        val leftChild = extractSegmentFilter(left, col)
        val rightChild = extractSegmentFilter(right, col)

        //if there exists leaf-node that doesn't contain partition column, the parent filter is
        //unnecessary for segment prunning.
        //e.g. "where a = xxx or partition = xxx", we can't filter any segment
        if (leftChild.eq(None) || rightChild.eq(None)) {
          None
        } else {
          Some(expressions.Or(leftChild.get, rightChild.get))
        }
      case expressions.And(left, right) =>
        val leftChild = extractSegmentFilter(left, col)
        val rightChild = extractSegmentFilter(right, col)

        //if there is only one leaf-node that contains partition column
        //e.g. "where a = xxx and partition = xxx",
        //then we can filter segment using "where partition = xxx"
        if (!leftChild.eq(None) && !rightChild.eq(None)) {
          Some(expressions.And(leftChild.get, rightChild.get))
        } else if (!rightChild.eq(None)) {
          rightChild
        } else if (!leftChild.eq(None)) {
          leftChild
        } else {
          None
        }
      case _ =>
        //other unary filter like EqualTo, GreaterThan, GreaterThanOrEqual, etc.
        if (filter.references.subsetOf(AttributeSet(col))) {
          Some(filter)
        } else {
          None
        }
    }
  }

  private def pruneSegments(filters: Seq[Expression],
                            segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {

    val filteredStatuses = if (filters.isEmpty) {
      segDirs
    } else {
      val translatedFilter = filters.map(filter => convertCastFilter(filter))
        .flatMap(ExpressionUtils.translateFilter)
      if (translatedFilter.isEmpty) {
        logInfo("Can not use filters to prune segments.")
        segDirs
      } else {
        val reducedFilter = translatedFilter.reduceLeft(And)
        val pruned = segDirs.filter {
          e => {
            val tsRange = cubeInstance.getSegment(e.segmentName, SegmentStatusEnum.READY).getTSRange
            SegFilters(tsRange.startValue, tsRange.endValue, pattern)
              .foldFilter(reducedFilter) match {
              case AlwaysTrue => true
              case AlwaysFalse => false
            }
          }
        }
        logInfo(s"Selected files after segments pruning:" + pruned.map(_.segmentName))
        pruned
      }
    }
    filteredStatuses
  }

  private def pruneShards(filters: Seq[Expression],
                          segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
    val filteredStatuses = if (layoutEntity.getShardByColumns.size() != 1) {
      segDirs
    } else {
      val normalizedFiltersAndExpr = filters.reduce(expressions.And)

      val pruned = segDirs.map { case SegmentDirectory(segName, segIdentifier, files) =>
        val segment = cubeInstance.getSegment(segName, SegmentStatusEnum.READY);
        val partitionNumber = segment.getCuboidShardNum(layoutEntity.getId).toInt
        require(partitionNumber > 0, "Shards num with shard by col should greater than 0.")

        val bitSet = getExpressionShards(normalizedFiltersAndExpr, shardByColumn.name,
          partitionNumber)

        val selected = files.filter(f => {
          val partitionId = FilePruner.getPartitionId(f.getPath)
          bitSet.get(partitionId)
        })
        SegmentDirectory(segName, segIdentifier, selected)
      }
      logInfo(s"Selected files after shards pruning:" + pruned.flatMap(_.files)
        .map(_.getPath.toString).mkString(";"))
      pruned
    }
    filteredStatuses
  }

  override lazy val inputFiles: Array[String] = Array.empty[String]

  override lazy val sizeInBytes: Long = {
    cubeInstance.getSegments.asScala
      .filter(_.getStatus.equals(SegmentStatusEnum.READY))
      .map(_.getSizeKB * 1024)
      .sum
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
      iter.map(v => getShardNumber(attr, v))
        .foreach(shardNum => matchedShards.set(shardNum))
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
      case expressions.InSet(a: Attribute, hset)
        if hset.forall(_.isInstanceOf[Literal]) && a.name == shardColumnName =>
        getShardSetFromIterable(a, hset.map(e => expressions.Literal(e).eval(EmptyRow)))
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
      case expressions.EqualTo(expressions.Cast(a: Attribute, _, _), Literal(v, t)) =>
        expressions.EqualTo(a, Literal(v, t))
      case expressions.EqualTo(Literal(v, t), expressions.Cast(a: Attribute, _, _)) =>
        expressions.EqualTo(Literal(v, t), a)
      case expressions.GreaterThan(expressions.Cast(a: Attribute, _, _), Literal(v, t)) =>
        expressions.GreaterThan(a, Literal(v, t))
      case expressions.GreaterThan(Literal(v, t), expressions.Cast(a: Attribute, _, _)) =>
        expressions.GreaterThan(Literal(v, t), a)
      case expressions.LessThan(expressions.Cast(a: Attribute, _, _), Literal(v, t)) =>
        expressions.LessThan(a, Literal(v, t))
      case expressions.LessThan(Literal(v, t), expressions.Cast(a: Attribute, _, _)) =>
        expressions.LessThan(Literal(v, t), a)
      case expressions.GreaterThanOrEqual(expressions.Cast(a: Attribute, _, _), Literal(v, t)) =>
        expressions.GreaterThanOrEqual(a, Literal(v, t))
      case expressions.GreaterThanOrEqual(Literal(v, t), expressions.Cast(a: Attribute, _, _)) =>
        expressions.GreaterThanOrEqual(Literal(v, t), a)
      case expressions.LessThanOrEqual(expressions.Cast(a: Attribute, _, _), Literal(v, t)) =>
        expressions.LessThanOrEqual(a, Literal(v, t))
      case expressions.LessThanOrEqual(Literal(v, t), expressions.Cast(a: Attribute, _, _)) =>
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
}

case class SegFilters(start: Long, end: Long, pattern: String) extends Logging {

  private def insurance(value: Any)
                       (func: Long => Filter): Filter = {
    value match {
      case v: Date =>
        // see SPARK-27546
        val ts = DateFormat.stringToMillis(v.toString)
        func(ts)
      case v @ (_:String | _: Int | _: Long) if pattern != null =>
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
          case And(AlwaysFalse, _) => Trivial(false)
          case And(_, AlwaysFalse) => Trivial(false)
          case And(AlwaysTrue, right) => right
          case And(left, AlwaysTrue) => left
          case other => other
        }
      case Or(left: Filter, right: Filter) =>
        Or(foldFilter(left), foldFilter(right)) match {
          case Or(AlwaysTrue, _) => Trivial(true)
          case Or(_, AlwaysTrue) => Trivial(true)
          case Or(AlwaysFalse, right) => right
          case Or(left, AlwaysFalse) => left
          case other => other
        }
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
  def Trivial(value: Boolean): Filter = {
    if (value) AlwaysTrue else AlwaysFalse
  }
}