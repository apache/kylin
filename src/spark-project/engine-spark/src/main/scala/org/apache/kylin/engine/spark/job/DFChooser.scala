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

import org.apache.kylin.guava30.shaded.common.base.Preconditions
import org.apache.kylin.guava30.shaded.common.collect.{Lists, Maps}
import org.apache.kylin.engine.spark.builder._
import org.apache.kylin.engine.spark.utils.SparkDataSource._
import org.apache.kylin.metadata.cube.cuboid.{NCuboidLayoutChooser, NSpanningTree}
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.NDataModel
import org.apache.kylin.metadata.model.NDataModel.TableKind
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.metadata.model.{SegmentStatusEnum, TblColRef}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.datasource.storage.StorageStoreUtils
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class DFChooser(toBuildTree: NSpanningTree,
                var seg: NDataSegment,
                jobId: String,
                ss: SparkSession,
                config: KylinConfig,
                needEncoding: Boolean)
  extends Logging {
  var reuseSources: java.util.Map[java.lang.Long, NBuildSourceInfo] =
    Maps.newHashMap[java.lang.Long, NBuildSourceInfo]()
  var flatTableSource: NBuildSourceInfo = _
  val flatTableDesc =
    new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, DFChooser.needJoinLookupTables(seg.getModel, toBuildTree))

  @throws[Exception]
  def decideSources(): Unit = {
    var map = Map.empty[Long, NBuildSourceInfo]
    toBuildTree.getRootIndexEntities.asScala
      .foreach { desc =>
        val layout = NCuboidLayoutChooser.selectLayoutForBuild(seg, desc)

        if (layout != null) {
          if (map.contains(layout.getId)) {
            map.apply(layout.getId).addCuboid(desc)
          } else {
            val nBuildSourceInfo = getSourceFromLayout(layout, desc)
            map += (layout.getId -> nBuildSourceInfo)
          }
        } else {
          if (flatTableSource == null) {
            flatTableSource = getFlatTable()
          }
          flatTableSource.addCuboid(desc)
        }
      }
    map.foreach(entry => reuseSources.put(entry._1, entry._2))
  }

  def computeColumnBytes(): mutable.HashMap[String, Long] = {
    computeColumnBytes(flatTableSource.getFlattableDS)
  }

  def computeColumnBytes(df: Dataset[Row]): mutable.HashMap[String, Long] = {
    val cols = flatTableDesc.getAllColumns
    val columns = df.columns
    val length = columns.length
    val result = new mutable.HashMap[String, Long]
    val rows = config.getCapacitySampleRows
    df.take(rows).foreach(row => {
      var i = 0
      for (i <- 0 until length) {
        if (!columns(i).contains(DFBuilderHelper.ENCODE_SUFFIX)) {
          val columnName = cols.get(i).getCanonicalName
          val value = row.get(i)
          val strValue = if (value == null) null
          else value.toString
          val bytes = DFChooser.utf8Length(strValue)
          if (result.get(columnName).isEmpty) result(columnName) = bytes
          else result(columnName) = bytes + result(columnName)
        }
      }
    }
    )
    result
  }

  def persistFlatTableIfNecessary(): String = {
    var path = ""
    if (shouldPersistFlatTable()) {
      val df = flatTableSource.getFlattableDS
      val columns = new mutable.ListBuffer[String]
      val columnIndex = df.schema.fieldNames.zipWithIndex.map(tp => (tp._2, tp._1)).toMap
      flatTableSource.getToBuildCuboids.asScala.foreach { c =>
        columns.appendAll(c.getDimensions.asScala.map(_.toString))

        c.getEffectiveMeasures.asScala.foreach { mea =>
          val parameters = mea._2.getFunction.getParameters.asScala
          if (parameters.head.isColumnType) {
            val measureUsedCols = parameters.map(p => columnIndex.apply(flatTableDesc.getColumnIndex(p.getColRef)).toString)
            columns.appendAll(measureUsedCols)
          }
        }
      }
      val toBuildCuboidColumns = columns.distinct.sorted
      logInfo(s"to build cuboid columns are $toBuildCuboidColumns")
      if (df.schema.nonEmpty) {
        val builtFaltTableBefore = seg.isFlatTableReady
        path = s"${config.getFlatTableDir(seg.getProject, seg.getDataflow.getId, seg.getId)}"
        if (!shouldUpdateFlatTable(new Path(path), df)) {
          logInfo(s"Skip already persisted flat table, segment: ${seg.getId} of dataflow: ${seg.getDataflow.getId}")
        } else {
          ss.sparkContext.setJobDescription("Persist flat table.")
          df.write.mode(SaveMode.Overwrite).parquet(path)
          // checkpoint
          DFBuilderHelper.checkPointSegment(seg, (copied: NDataSegment) => {
            copied.setFlatTableReady(true)
            if (builtFaltTableBefore) {
              // if flat table is updated, there might be some data inconsistency across indexes
              copied.setStatus(SegmentStatusEnum.WARNING)
            }
          })
        }
        flatTableSource.setParentStorageDF(ss.read.parquet(path))
      }
    }
    path
  }

  private def persistFactViewIfNecessary(): String = {
    var path = ""
    if (needEncoding && config.isPersistFlatViewEnabled) {
      logInfo(s"Check project:${seg.getProject} seg:${seg.getName} persist view fact table.")
      val fact = flatTableDesc.getDataModel.getRootFactTable
      val globalDicts = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree.getAllIndexEntities)
      val existsFactDictCol = globalDicts.asScala.exists(_.getTableRef.getTableIdentity.equals(fact.getTableIdentity))

      logInfo(s"Fact table ${fact.getAlias} isView ${fact.getTableDesc.isView} and fact table exists dict col ${existsFactDictCol}")
      if (fact.getTableDesc.isView && existsFactDictCol) {
        var viewDS = ss.table(fact.getTableDesc).alias(fact.getAlias)
        path = s"${config.getJobTmpFactTableViewDir(seg.getProject, jobId)}"

        if (seg.isFactViewReady && HadoopUtil.getWorkingFileSystem.exists(new Path(path))) {
          logInfo(s"Skip already persisted fact view, segment: ${seg.getId} of dataflow: ${seg.getDataflow.getId}")
        } else {
          ss.sparkContext.setJobDescription("Persist view fact table.")
          viewDS = FlatTableHelper.applyPartitionDesc(flatTableDesc, viewDS, false)
          viewDS = FlatTableHelper.applyFilterCondition(flatTableDesc, viewDS, false)
          viewDS.write.mode(SaveMode.Overwrite).parquet(path)
          logInfo(s"Persist view fact table into:$path.")

          // checkpoint fact view
          DFBuilderHelper.checkPointSegment(seg, (copied: NDataSegment) => copied.setFactViewReady(true))
        }
        // end of fact view persist
      }
    }
    path
  }

  private def getSourceFromLayout(layout: LayoutEntity,
                                  indexEntity: IndexEntity) = {
    val buildSource = new NBuildSourceInfo
    val segDetails = seg.getSegDetails
    val dataCuboid = segDetails.getLayoutById(layout.getId)
    Preconditions.checkState(dataCuboid != null)
    buildSource.setParentStorageDF(StorageStoreUtils.toDF(seg, layout, ss))
    buildSource.setSparkSession(ss)
    buildSource.setCount(dataCuboid.getRows)
    buildSource.setLayoutId(layout.getId)
    buildSource.setByteSize(dataCuboid.getByteSize)
    buildSource.addCuboid(indexEntity)
    logInfo(
      s"Reuse a suitable layout: ${layout.getId} for building cuboid: ${indexEntity.getId}")
    buildSource
  }

  @throws[Exception]
  private def getFlatTable(): NBuildSourceInfo = {
    val viewPath = persistFactViewIfNecessary()
    val sourceInfo = new NBuildSourceInfo
    sourceInfo.setSparkSession(ss)
    sourceInfo.setLayoutId(DFChooser.FLAT_TABLE_FLAG)
    sourceInfo.setViewFactTablePath(viewPath)

    val needJoin = DFChooser.needJoinLookupTables(seg.getModel, toBuildTree)
    val flatTableDesc = new NCubeJoinedFlatTableDesc(seg.getIndexPlan, seg.getSegRange, needJoin)
    val flatTable = new CreateFlatTable(flatTableDesc, seg, toBuildTree, ss, sourceInfo)
    val afterJoin: Dataset[Row] = flatTable.generateDataset(needEncoding, needJoin)
    sourceInfo.setFlattableDS(afterJoin)
    sourceInfo.setAllColumns(flatTableDesc.getAllColumns)
    sourceInfo
  }

  def shouldPersistFlatTable(): Boolean = {
    if (flatTableSource == null) {
      false
    } else if (config.isPersistFlatTableEnabled) {
      true
    } else if (flatTableSource.getToBuildCuboids.size() > config.getPersistFlatTableThreshold) {
      true
    } else {
      false
    }
  }

  def shouldUpdateFlatTable(flatTablePath: Path, df: DataFrame): Boolean = {
    if (seg.isFlatTableReady && HadoopUtil.getWorkingFileSystem.exists(flatTablePath)) {
      val curr = df.schema.fieldNames

      val flatDF: DataFrame = Try(ss.read.parquet(flatTablePath.toString)) match {
        case Success(df) => df
        case Failure(f) =>
          logInfo(s"Handled AnalysisException: Unable to infer schema for Parquet. Flat table path $flatTablePath is empty", f)
          ss.emptyDataFrame
      }
      val prev = flatDF.schema.fieldNames
      if (curr.forall(prev.contains(_))) {
        logInfo(s"Reuse persisted flat table on dataFlow: ${seg.getDataflow.getId}, segment: ${seg.getId}." +
          s" Prev schema: [${prev.mkString(", ")}], curr schema: [${curr.mkString(", ")}]")
        false
      } else {
        logInfo(s"Need to update flat table on dataFlow: ${seg.getDataflow.getId}, segment: ${seg.getId}." +
          s" Prev schema: [${prev.mkString(", ")}], curr schema: [${curr.mkString(", ")}]")
        true
      }
    } else {
      true
    }
  }
}


object DFChooser extends Logging {
  def apply(toBuildTree: NSpanningTree,
            seg: NDataSegment,
            jobId: String,
            ss: SparkSession,
            config: KylinConfig,
            needEncoding: Boolean,
            ignoredSnapshotTables: java.util.Set[String]): DFChooser =
    new DFChooser(toBuildTree: NSpanningTree,
      seg: NDataSegment,
      jobId,
      ss: SparkSession,
      config: KylinConfig,
      needEncoding)

  val FLAT_TABLE_FLAG: Long = -1L

  def utf8Length(sequence: CharSequence): Int = {
    var count = 0
    var i = 0
    if (sequence != null) {
      val len = sequence.length
      while (i < len) {
        val ch = sequence.charAt(i)
        if (ch <= 0x7F) count += 1
        else if (ch <= 0x7FF) count += 2
        else if (Character.isHighSurrogate(ch)) {
          count += 4
          i += 1
        }
        else count += 3
        i += 1
      }
    }
    count
  }


  def needJoinLookupTables(model: NDataModel, toBuildTree: NSpanningTree): Boolean = {
    val conf = KylinConfig.getInstanceFromEnv
    if (!conf.isFlatTableJoinWithoutLookup) {
      return true
    }

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      return true
    }

    val joinTables = model.getJoinTables
    val factTable = model.getRootFactTable
    var needJoin = false
    if (joinTables.asScala.filter(_.getJoin.isLeftJoin).size != joinTables.size()) {
      needJoin = true
    }

    if (joinTables.asScala.filter(_.getKind == TableKind.LOOKUP).size != joinTables.size()) {
      needJoin = true
    }

    val toBuildCols = Lists.newArrayList[TblColRef]()
    toBuildTree.getRootIndexEntities.asScala.map(
      index => {
        index.getEffectiveMeasures.asScala.map(
          mea =>
            toBuildCols.addAll(mea._2.getFunction.getColRefs)
        )
        index.getEffectiveDimCols.asScala.map(
          dim =>
            toBuildCols.add(dim._2)
        )
      }
    )
    toBuildCols.asScala.distinct.map(
      col =>
        if (!factTable.getTableIdentity.equalsIgnoreCase(col.getTableRef.getTableIdentity)) {
          needJoin = true
        }
    )

    needJoin
  }
}
