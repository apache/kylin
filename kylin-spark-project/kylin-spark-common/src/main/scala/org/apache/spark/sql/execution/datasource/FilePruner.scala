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
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree
import org.apache.kylin.engine.spark.metadata.cube.{ManagerHub, PathManager}
import org.apache.kylin.engine.spark.metadata.MetadataConverter
import org.apache.kylin.metadata.model.{PartitionDesc, SegmentStatusEnum}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, EmptyRow, Expression, Literal}
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.utils.SparkTypeUtil
import org.apache.spark.util.collection.BitSet

import scala.collection.JavaConversions
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
case class ShardSpec(
                      numShards: Int,
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

class FilePruner(
                  cubeInstance: CubeInstance,
                  cuboid: Cuboid,
                  val session: SparkSession,
                  val options: Map[String, String])
  extends FileIndex with ResetShufflePartition with Logging {

  private lazy val segmentDirs: Seq[SegmentDirectory] = {
    cubeInstance.getSegments.asScala
      .filter(_.getStatus.equals(SegmentStatusEnum.READY))
      .map(seg => SegmentDirectory(seg.getName, seg.getStorageLocationIdentifier, null))
    cubeInstance.getSegments.asScala
      .filter(_.getStatus.equals(SegmentStatusEnum.READY)).map(seg => {
      val segName = seg.getName
      val path = PathManager.getParquetStoragePath(cubeInstance, segName, seg.getStorageLocationIdentifier, layoutEntity.getId)
      val files = new InMemoryFileIndex(session,
        Seq(new Path(path)),
        options,
        Some(dataSchema),
        FileStatusCache.getOrCreate(session))
        .listFiles(Nil, Nil)
        .flatMap(_.files)
        .filter(_.isFile)
      SegmentDirectory(segName, seg.getStorageLocationIdentifier, files)
    }).filter(_.files.nonEmpty)
  }

  val layoutEntity = MetadataConverter.toLayoutEntity(cubeInstance, cuboid)

  val dataSchema: StructType = {
    StructType(layoutEntity.getOrderedDimensions.values().asScala
      .map { column => StructField(column.id.toString, column.dataType) }
      .toSeq ++
      layoutEntity.getOrderedMeasures.asScala
        .map { entry => StructField(entry._1.toString, SparkTypeUtil.generateFunctionReturnDataType(entry._2)) }
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
        val id = layoutEntity.getOrderedDimensions.asScala.values.find(column => column.columnName.equals(ref.getName))
        if (id.isDefined && (ref.getType.isDateTimeFamily || ref.getType.isStringFamily)) {
          if (ref.getType.isDateTimeFamily) {
            pattern = desc.getPartitionDateFormat
          }
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

  def getShardSpec: Option[ShardSpec] = {
    None
  }

  var cached = new java.util.HashMap[(Seq[Expression], Seq[Expression]), Seq[PartitionDirectory]]()

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (cached.containsKey((partitionFilters, dataFilters))) {
      return cached.get((partitionFilters, dataFilters))
    }

    require(isResolved)
    val timePartitionFilters = getSpecFilter(dataFilters, timePartitionColumn)
    logInfo(s"Applying time partition filters: ${timePartitionFilters.mkString(",")}")

    val fsc = ShardFileStatusCache.getFileStatusCache(session)

    // segment pruning
    var selected = afterPruning("segment", timePartitionFilters, segmentDirs) {
      pruneSegments
    }
    //    QueryContextFacade.current().record("seg_pruning")
    selected = selected.par.map { e =>
      val path = new Path(toPath(e.segmentName, e.identifier))
      val maybeStatuses = fsc.getLeafFiles(path)
      if (maybeStatuses.isDefined) {
        SegmentDirectory(e.segmentName, e.identifier, maybeStatuses.get)
      } else {
        val statuses = path.getFileSystem(session.sparkContext.hadoopConfiguration).listStatus(path)
        fsc.putLeafFiles(path, statuses)
        SegmentDirectory(e.segmentName, e.identifier, statuses)
      }
    }.toIterator.toSeq
    //    QueryContextFacade.current().record("fetch_file_status")
    // shards pruning
    selected = afterPruning("shard", dataFilters, selected) {
      pruneShards
    }
    //    QueryContextFacade.current().record("shard_pruning")
    val totalFileSize = selected.flatMap(partition => partition.files).map(_.getLen).sum
    logInfo(s"totalFileSize is ${totalFileSize}")
    setShufflePartitions(totalFileSize, session)
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

  private def afterPruning(pruningType: String, specFilters: Seq[Expression], inputs: Seq[SegmentDirectory])
                          (pruningFunc: (Seq[Expression], Seq[SegmentDirectory]) => Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
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

  private def getSpecFilter(dataFilters: Seq[Expression], col: Attribute): Seq[Expression] = {
    dataFilters.filter(_.references.subsetOf(AttributeSet(col)))
  }

  private def pruneSegments(
                             filters: Seq[Expression],
                             segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {

    val filteredStatuses = if (filters.isEmpty) {
      segDirs
    } else {
      val reducedFilter = filters.flatMap(DataSourceStrategy.translateFilter).reduceLeft(And)
      segDirs.filter {
        e => {
          val tsRange = cubeInstance.getSegment(e.segmentName, SegmentStatusEnum.READY).getTSRange
          SegFilters(tsRange.startValue, tsRange.endValue, pattern).foldFilter(reducedFilter) match {
            case Trivial(true) => true
            case Trivial(false) => false
          }
        }
      }
    }
    logInfo(s"Selected files after segments pruning:" + filteredStatuses.map(_.segmentName))
    filteredStatuses
  }

    private def pruneShards(
      filters: Seq[Expression],
      segDirs: Seq[SegmentDirectory]): Seq[SegmentDirectory] = {
      val filteredStatuses = if (layoutEntity.getShardByColumns.size() != 1) {
        segDirs
      } else {
        val normalizedFiltersAndExpr = filters.reduce(expressions.And)

        val pruned = segDirs.map { case SegmentDirectory(segName, segIdentifier, files) =>
          val segment = cubeInstance.getSegment(segName, SegmentStatusEnum.READY);
          val segID = segment.getUuid
          val segmentInfo = ManagerHub.getSegmentInfo(cubeInstance.getConfig, cubeInstance.getId, segID)
          val spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(segmentInfo.toBuildLayouts))
          //val partitionNumber = spanningTree.getLayoutEntity(layoutEntity.getId).getShardNum
          val partitionNumber = segment.getCuboidShardNum(layoutEntity.getId).toInt
          require(partitionNumber > 0, "Shards num with shard by col should greater than 0.")

          val bitSet = getExpressionShards(normalizedFiltersAndExpr, shardByColumn.name, partitionNumber)

          val selected = files.filter(f => {
            val partitionId = FilePruner.getPartitionId(f.getPath)
            bitSet.get(partitionId)
          })
          SegmentDirectory(segName, segIdentifier, selected)
        }
        logInfo(s"Selected files after shards pruning:" + pruned.flatMap(_.files).map(_.getPath.toString).mkString(";"))
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

  private def getExpressionShards(
                                   expr: Expression,
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
}

case class Trivial(value: Boolean) extends Filter {
  override def references: Array[String] = findReferences(value)
}