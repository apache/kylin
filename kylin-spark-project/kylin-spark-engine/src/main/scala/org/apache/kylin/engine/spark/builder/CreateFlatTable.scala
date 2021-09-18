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

package org.apache.kylin.engine.spark.builder

import java.util.Locale

import org.apache.kylin.shaded.com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.engine.spark.builder.CubeBuilderHelper.{ENCODE_SUFFIX, _}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil._
import org.apache.kylin.engine.spark.metadata._
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree
import org.apache.kylin.engine.spark.utils.SparkDataSource._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

class CreateFlatTable(val seg: SegmentInfo,
                      val toBuildTree: SpanningTree,
                      val ss: SparkSession,
                      val sourceInfo: NBuildSourceInfo,
                      val jobId: String) extends Logging {

  import org.apache.kylin.engine.spark.builder.CreateFlatTable._

  def generateDataset(needEncode: Boolean = false, needJoin: Boolean = true): Dataset[Row] = {

    val ccCols = seg.allColumns.filter(_.isInstanceOf[ComputedColumnDesc]).toSet
    var rootFactDataset = generateTableDataset(seg.factTable, ccCols.toSeq, ss, seg.project)

    logInfo(s"Create flat table need join lookup tables $needJoin, need encode cols $needEncode")
    rootFactDataset = applyPartitionCondition(seg, rootFactDataset)

    (needJoin, needEncode) match {
      case (true, true) =>
        val (toBuildDictSet, globalDictSet): GlobalDictType = (seg.toBuildDictColumns, seg.allDictColumns)
        rootFactDataset = encodeWithCols(rootFactDataset, ccCols, toBuildDictSet, globalDictSet)
        val encodedLookupMap = generateLookupTableDataset(seg, ccCols.toSeq, ss)
          .map(lp => (lp._1, encodeWithCols(lp._2, ccCols, toBuildDictSet, globalDictSet)))

        val allTableDataset = Seq(rootFactDataset) ++ encodedLookupMap.map(_._2)

        rootFactDataset = joinFactTableWithLookupTables(rootFactDataset, encodedLookupMap, seg, ss)
        // KYLIN-4965: Must apply filter conditions after join lookup tables,
        // as there maybe some filter columns which are belonged to lookup tables.
        rootFactDataset = applyFilterCondition(seg, rootFactDataset)
        rootFactDataset = encodeWithCols(rootFactDataset,
          filterCols(allTableDataset, ccCols),
          filterCols(allTableDataset, toBuildDictSet),
          filterCols(allTableDataset, globalDictSet))
      case (true, false) =>
        val lookupTableDatasetMap = generateLookupTableDataset(seg, ccCols.toSeq, ss)
        rootFactDataset = joinFactTableWithLookupTables(rootFactDataset, lookupTableDatasetMap, seg, ss)
        // KYLIN-4965: Must apply filter conditions after join lookup tables,
        // as there maybe some filter columns which are belonged to lookup tables.
        rootFactDataset = applyFilterCondition(seg, rootFactDataset)
        rootFactDataset = withColumn(rootFactDataset, ccCols)
      case (false, true) =>
        val (dictCols, encodeCols) = (seg.toBuildDictColumns, seg.allDictColumns)
        rootFactDataset = encodeWithCols(rootFactDataset, ccCols, dictCols, encodeCols)
      case _ =>
    }
    changeSchemeToColumnIndice(rootFactDataset, seg)
  }

  private def encodeWithCols(ds: Dataset[Row],
                             ccCols: Set[ColumnDesc],
                             dictCols: Set[ColumnDesc],
                             encodeCols: Set[ColumnDesc]): Dataset[Row] = {
    val ccDataset = withColumn(ds, ccCols)
    buildDict(ccDataset, dictCols)
    encodeColumn(ccDataset, encodeCols)
  }

  private def withColumn(ds: Dataset[Row], withCols: Set[ColumnDesc]): Dataset[Row] = {
    val matchedCols = filterCols(ds, withCols)
    var withDs = ds
    matchedCols.foreach(m => withDs = withDs.withColumn(convertFromDot(m.identity),
      expr(convertFromDot(m.asInstanceOf[ComputedColumnDesc].expression))))
    withDs
  }

  private def buildDict(ds: Dataset[Row], dictCols: Set[ColumnDesc]): Unit = {
    val matchedCols = filterCols(ds, dictCols)
    if (!matchedCols.isEmpty) {
      val builder = new CubeDictionaryBuilder(ds, seg, ss, Sets.newHashSet(matchedCols.asJavaCollection))
      builder.buildDictSet()
    }
  }

  private def encodeColumn(ds: Dataset[Row], encodeCols: Set[ColumnDesc]): Dataset[Row] = {
    val matchedCols = filterCols(ds, encodeCols)
    var encodeDs = ds
    if (!matchedCols.isEmpty) {
      encodeDs = CubeTableEncoder.encodeTable(ds, seg, matchedCols.asJava, jobId)
    }
    encodeDs
  }
}

object CreateFlatTable extends Logging {
  type GlobalDictType = (Set[ColumnDesc], Set[ColumnDesc])

  private def generateTableDataset(tableInfo: TableDesc,
                                   cols: Seq[ColumnDesc],
                                   ss: SparkSession,
                                   project: String = null,
                                   sourceInfo: NBuildSourceInfo = null) = {
    var dataset: Dataset[Row] =
      if (sourceInfo != null && !StringUtils.isBlank(sourceInfo.getViewFactTablePath)) {
        ss.read.parquet(sourceInfo.getViewFactTablePath).alias(tableInfo.alias)
      } else {
        ss.table(tableInfo).alias(tableInfo.alias)
      }

    val suitableCols = chooseSuitableCols(dataset, cols)
    dataset = changeSchemaToAliasDotName(dataset, tableInfo.alias)
    val selectedCols = dataset.schema.fields.map(tp => col(tp.name)) ++ suitableCols
    logInfo(s"Table ${tableInfo.alias} schema ${dataset.schema.treeString}")
    dataset.select(selectedCols: _*)
  }

  private def generateLookupTableDataset(desc: SegmentInfo,
                                         cols: Seq[ColumnDesc],
                                         ss: SparkSession): Array[(JoinDesc, Dataset[Row])] = {
    desc.joindescs.map {
      joinDesc =>
        (joinDesc, generateTableDataset(joinDesc.lookupTable, cols, ss))
    }
  }

  private def applyFilterCondition(desc: SegmentInfo, ds: Dataset[Row]): Dataset[Row] = {
    var afterFilter = ds
    if (StringUtils.isNotBlank(desc.filterCondition)) {
      val afterConvertCondition = desc.filterCondition
      logInfo(s"Filter condition is $afterConvertCondition")
      afterFilter = afterFilter.where(afterConvertCondition)
    }
    afterFilter
  }

  private def applyPartitionCondition(desc: SegmentInfo, ds: Dataset[Row]): Dataset[Row] = {
    var afterFilter = ds
    if (StringUtils.isNotBlank(desc.partitionExp)) {
      logInfo(s"Partition Filter condition is ${desc.partitionExp}")
      afterFilter = afterFilter.where(desc.partitionExp)
    }
    afterFilter
  }

  def joinFactTableWithLookupTables(rootFactDataset: Dataset[Row],
                                    lookupTableDatasetMap: Array[(JoinDesc, Dataset[Row])],
                                    buildDesc: SegmentInfo,
                                    ss: SparkSession): Dataset[Row] = {
    lookupTableDatasetMap.foldLeft(rootFactDataset)(
      (joinedDataset: Dataset[Row], tuple: (JoinDesc, Dataset[Row])) =>
        joinTableDataset(buildDesc.factTable, tuple._1, joinedDataset, tuple._2, ss))
  }

  def joinTableDataset(rootFactDesc: TableDesc,
                       joinDesc: JoinDesc,
                       rootFactDataset: Dataset[Row],
                       lookupDataset: Dataset[Row],
                       ss: SparkSession): Dataset[Row] = {
    var afterJoin = rootFactDataset
    val joinType = joinDesc.joinType
    if (StringUtils.isNotEmpty(joinType)) {
      val pk = joinDesc.PKS
      val fk = joinDesc.FKS
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$JoinDesc, pk: ${pk.mkString(",")}")
      }
      val equivConditionColPairs = fk.zip(pk).map(joinKey =>
        col(convertFromDot(joinKey._1.identity))
          .equalTo(col(convertFromDot(joinKey._2.identity))))
      logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")

      val condition = equivConditionColPairs.reduce(_ && _)
      logInfo(s"Root table ${rootFactDesc.identity}, join table ${joinDesc.lookupTable.alias}, condition: ${condition.toString()}")
      afterJoin = afterJoin.join(lookupDataset, condition, joinType)
    }
    afterJoin
  }

  def changeSchemeToColumnIndice(ds: Dataset[Row], desc: SegmentInfo): Dataset[Row] = {
    val structType = ds.schema
    val columnNameToIndex = desc.allColumns
      .map(column => convertFromDot(column.identity))
      .zip(desc.allColumns.map(_.id))
    val columnToIndexMap = columnNameToIndex.toMap
    val encodeSeq = structType.filter(_.name.endsWith(ENCODE_SUFFIX)).map {
      tp =>
        val originNam = tp.name.replaceFirst(ENCODE_SUFFIX, "")
        val index = columnToIndexMap.apply(originNam)
        col(tp.name).alias(index.toString + ENCODE_SUFFIX)
    }
    val columns = columnNameToIndex.map(tp => expr(tp._1).alias(tp._2.toString))
    logInfo(s"Select model column is ${columns.mkString(",")}")
    logInfo(s"Select model encoding column is ${encodeSeq.mkString(",")}")
    val selectedColumns = columns ++ encodeSeq

    logInfo(s"Select model all column is ${selectedColumns.mkString(",")}")
    ds.select(selectedColumns: _*)
  }

  def replaceDot(original: String, columns: List[ColumnDesc]): String = {
    val sb = new StringBuilder(original)

    for (namedColumn <- columns) {
      var start = 0
      while (sb.toString.toLowerCase(Locale.ROOT).indexOf(
        namedColumn.identity.toLowerCase(Locale.ROOT)) != -1) {
        start = sb.toString.toLowerCase(Locale.ROOT)
          .indexOf(namedColumn.identity.toLowerCase(Locale.ROOT))
        sb.replace(start,
          start + namedColumn.identity.length,
          convertFromDot(namedColumn.identity))
      }
    }
    sb.toString()
  }

  def changeSchemaToAliasDotName(original: Dataset[Row], alias: String): Dataset[Row] = {
    val sf = original.schema.fields
    val newSchema = sf
      .map(field => convertFromDot(alias + "." + field.name))
      .toSeq
    val newdf = original.toDF(newSchema: _*)
    logInfo(s"After change alias from ${original.schema.treeString} to ${newdf.schema.treeString}")
    newdf
  }

}
