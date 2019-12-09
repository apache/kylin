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

package io.kyligence.kap.engine.spark.builder

import java.util
import java.util.Locale

import com.google.common.collect.Sets
import io.kyligence.kap.engine.spark.builder.DFBuilderHelper.{ENCODE_SUFFIX, _}
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil._
import io.kyligence.kap.engine.spark.utils.SparkDataSource._
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.engine.spark.metadata.cube.model.{CubeJoinedFlatTableDesc, DataModel, DataSegment, IJoinedFlatTableDesc, JoinDesc, JoinTableDesc, PartitionDesc, SegmentRange, SpanningTree, TableDesc, TableRef, TblColRef}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class CreateFlatTable(val flatTable: IJoinedFlatTableDesc,
                      val seg: DataSegment,
                      val toBuildTree: SpanningTree,
                      val ss: SparkSession,
                      val sourceInfo: NBuildSourceInfo) extends Logging {

  import io.kyligence.kap.engine.spark.builder.CreateFlatTable._

  def generateDataset(needEncode: Boolean = false, needJoin: Boolean = true): Dataset[Row] = {
    val model = flatTable.getDataModel

    val ccCols = model.getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    var rootFactDataset = generateTableDataset(model.getRootFactTable, ccCols.toSeq, model.getRootFactTable.getAlias, ss)
    rootFactDataset = applyFilterCondition(flatTable, rootFactDataset)

    logInfo(s"Create flattable need join lookup tables ${needJoin}, need encode cols ${needEncode}")

    (needJoin, needEncode) match {
      case (true, true) =>
        val (dictCols, encodeCols): GlobalDictType = assemblyGlobalDictTuple(seg, toBuildTree)
        rootFactDataset = encodeWithCols(rootFactDataset, ccCols, dictCols, encodeCols)
        val encodedLookupMap = generateLookupTableDataset(model, ccCols.toSeq, ss)
          .map(lp => (lp._1, encodeWithCols(lp._2, ccCols, dictCols, encodeCols)))

        val allTableDataset = Seq(rootFactDataset) ++ encodedLookupMap.values

        rootFactDataset = joinFactTableWithLookupTables(rootFactDataset, encodedLookupMap, model, ss)
        rootFactDataset = encodeWithCols(rootFactDataset,
          filterCols(allTableDataset, ccCols),
          filterCols(allTableDataset, dictCols),
          filterCols(allTableDataset, encodeCols))
      case (true, false) =>
        val lookupTableDatasetMap = generateLookupTableDataset(model, ccCols.toSeq, ss)
        rootFactDataset = joinFactTableWithLookupTables(rootFactDataset, lookupTableDatasetMap, model, ss)
        rootFactDataset = withColumn(rootFactDataset, ccCols)
      case (false, true) =>
        val (dictCols, encodeCols) = assemblyGlobalDictTuple(seg, toBuildTree)
        rootFactDataset = encodeWithCols(rootFactDataset, ccCols, dictCols, encodeCols)
      case _ =>
    }

    flatTable match {
      case joined: CubeJoinedFlatTableDesc =>
        changeSchemeToColumnIndice(rootFactDataset, joined)
      case unsupported =>
        throw new UnsupportedOperationException(
          s"Unsupported flat table desc type : ${unsupported.getClass}.")
    }
  }

  private def encodeWithCols(ds: Dataset[Row],
                             ccCols: Set[TblColRef],
                             dictCols: Set[TblColRef],
                             encodeCols: Set[TblColRef]): Dataset[Row] = {
    val ccDataset = withColumn(ds, ccCols)
    buildDict(ccDataset, dictCols)
    encodeColumn(ccDataset, encodeCols)
  }

  private def withColumn(ds: Dataset[Row], withCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = filterCols(ds, withCols)
    var withDs = ds
    matchedCols.foreach(m => withDs = withDs.withColumn(convertFromDot(m.getIdentity),
      expr(convertFromDot(m.getExpressionInSourceDB))))
    withDs
  }

  private def buildDict(ds: Dataset[Row], dictCols: Set[TblColRef]): Unit = {
    val matchedCols = filterCols(ds, dictCols)
    val builder = new DFDictionaryBuilder(ds, seg, ss, Sets.newHashSet(matchedCols.asJavaCollection))
    builder.buildDictSet()
  }

  private def encodeColumn(ds: Dataset[Row], encodeCols: Set[TblColRef]): Dataset[Row] = {
    val matchedCols = filterCols(ds, encodeCols)
    var encodeDs = ds
    if (!matchedCols.isEmpty) {
      encodeDs = DFTableEncoder.encodeTable(ds, seg, matchedCols.asJava)
    }
    encodeDs
  }
}

object CreateFlatTable extends Logging {
  type GlobalDictType = (Set[TblColRef], Set[TblColRef])

  @throws(classOf[ParseException])
  @throws(classOf[AnalysisException])
  def generateFullFlatTable(model: DataModel, ss: SparkSession): Dataset[Row] = {
    val rootFact = model.getRootFactTable
    val ccCols = rootFact.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    val rootFactDataset = generateTableDataset(rootFact, ccCols.toSeq, rootFact.getAlias, ss)
    val lookupTableDataset = generateLookupTableDataset(model, ccCols.toSeq, ss)
    joinFactTableWithLookupTables(rootFactDataset, lookupTableDataset, model, ss)
  }

  private def generateTableDataset(tableRef: TableRef,
                                   cols: Seq[TblColRef],
                                   alias: String,
                                   ss: SparkSession,
                                   sourceInfo: NBuildSourceInfo = null) = {
    var dataset: Dataset[Row] =
      if (sourceInfo != null && !StringUtils.isBlank(sourceInfo.getViewFactTablePath)) {
        ss.read.parquet(sourceInfo.getViewFactTablePath)
      } else {
        ss.table(tableRef.getTableDesc).alias(alias)
      }

    val suitableCols = chooseSuitableCols(dataset, cols)
    dataset = changeSchemaToAliasDotName(dataset, alias)
    val selectedCols = dataset.schema.fields.map(tp => col(tp.name)) ++ suitableCols
    logInfo(s"Table ${tableRef.getAlias} schema ${dataset.schema.treeString}")
    dataset.select(selectedCols: _*)
  }

  private def generateLookupTableDataset(model: DataModel,
                                         cols: Seq[TblColRef],
                                         ss: SparkSession): mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = {
    val lookupTables = mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]]()
    model.getJoinTables.asScala.map(
      joinDesc => {
        val lookupTable = generateTableDataset(joinDesc.getTableRef, cols.toSeq, joinDesc.getAlias, ss)
        lookupTables.put(joinDesc, lookupTable)
      }
    )
    lookupTables
  }

  private def applyFilterCondition(flatTable: IJoinedFlatTableDesc, ds: Dataset[Row]): Dataset[Row] = {
    var afterFilter = ds
    val model = flatTable.getDataModel

    if (StringUtils.isNotBlank(model.getFilterCondition)) {
      var afterConvertCondition = model.getFilterCondition
      afterConvertCondition = replaceDot(model.getFilterCondition, model)
      logInfo(s"Filter condition is $afterConvertCondition")
      afterFilter = afterFilter.where(afterConvertCondition)
    }

    val partDesc = model.getPartitionDesc
    if (partDesc != null && partDesc.getPartitionDateColumn != null) {
      val segRange = flatTable.getSegRange
      if (segRange != null && !segRange.isInfinite) {
        var afterConvertPartition = partDesc.getPartitionConditionBuilder
          .buildDateRangeCondition(partDesc, null, segRange)
        afterConvertPartition = replaceDot(afterConvertPartition, model)
        logInfo(s"Partition filter $afterConvertPartition")
        afterFilter = afterFilter.where(afterConvertPartition) // TODO: mp not supported right now
      }
    }
    afterFilter
  }

  def joinFactTableWithLookupTables(rootFactDataset: Dataset[Row],
                                    lookupTableDatasetMap: mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]],
                                    model: DataModel,
                                    ss: SparkSession): Dataset[Row] = {
    lookupTableDatasetMap.foldLeft(rootFactDataset)(
      (joinedDataset: Dataset[Row], tuple: (JoinTableDesc, Dataset[Row])) =>
        joinTableDataset(model.getRootFactTable.getTableDesc, tuple._1, joinedDataset, tuple._2, ss))
  }

  def joinTableDataset(rootFactDesc: TableDesc,
                       lookupDesc: JoinTableDesc,
                       rootFactDataset: Dataset[Row],
                       lookupDataset: Dataset[Row],
                       ss: SparkSession): Dataset[Row] = {
    var afterJoin = rootFactDataset
    val join = lookupDesc.getJoin
    if (join != null && !StringUtils.isEmpty(join.getType)) {
      val joinType = join.getType.toUpperCase(Locale.ROOT)
      val pk = join.getPrimaryKeyColumns
      val fk = join.getForeignKeyColumns
      if (pk.length != fk.length) {
        throw new RuntimeException(
          s"Invalid join condition of fact table: $rootFactDesc,fk: ${fk.mkString(",")}," +
            s" lookup table:$lookupDesc, pk: ${pk.mkString(",")}")
      }
      val equiConditionColPairs = fk.zip(pk).map(joinKey =>
        col(convertFromDot(joinKey._1.getIdentity))
          .equalTo(col(convertFromDot(joinKey._2.getIdentity))))
      logInfo(s"Lookup table schema ${lookupDataset.schema.treeString}")

      if (join.getNonEquiJoinCondition != null) {
        var condition = NonEquiJoinConditionBuilder.convert(join.getNonEquiJoinCondition)
        if (!equiConditionColPairs.isEmpty) {
          condition = condition && equiConditionColPairs.reduce(_ && _)
        }
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, non-equi condition: ${condition.toString()}")
        afterJoin = afterJoin.join(lookupDataset, condition, joinType)
      } else {
        val condition = equiConditionColPairs.reduce(_ && _)
        logInfo(s"Root table ${rootFactDesc.getIdentity}, join table ${lookupDesc.getAlias}, condition: ${condition.toString()}")
        afterJoin = afterJoin.join(lookupDataset, condition, joinType)
      }
    }
    afterJoin
  }

  def changeSchemeToColumnIndice(ds: Dataset[Row], flatTable: CubeJoinedFlatTableDesc): Dataset[Row] = {
    val structType = ds.schema
    val colIndices = flatTable.getIndices.asScala
    val columnNameToIndex = flatTable.getAllColumns
      .asScala
      .map(column => convertFromDot(column.getIdentity))
      .zip(colIndices)
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

  def replaceDot(original: String, model: DataModel): String = {
    val sb = new StringBuilder(original)

    for (namedColumn <- model.getAllNamedColumns.asScala) {
      var start = 0
      while (sb.toString.toLowerCase(Locale.ROOT).indexOf(
        namedColumn.getAliasDotColumn.toLowerCase(Locale.ROOT)) != -1) {
        start = sb.toString.toLowerCase(Locale.ROOT)
          .indexOf(namedColumn.getAliasDotColumn.toLowerCase(Locale.ROOT))
        sb.replace(start,
          start + namedColumn.getAliasDotColumn.length,
          convertFromDot(namedColumn.getAliasDotColumn))
      }
    }
    sb.toString()
  }

  def assemblyGlobalDictTuple(seg: DataSegment, toBuildTree: SpanningTree): GlobalDictType = {
    val toBuildDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDictToBuild(seg, toBuildTree)
    val globalDictSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, toBuildTree)
    (toBuildDictSet.asScala.toSet, globalDictSet.asScala.toSet)
  }

  def changeSchemaToAliasDotName(original: Dataset[Row],
                                 alias: String): Dataset[Row] = {
    val sf = original.schema.fields
    val newSchema = sf
      .map(field => convertFromDot(alias + "." + field.name))
      .toSeq
    val newdf = original.toDF(newSchema: _*)
    logInfo(s"After change alias from ${original.schema.treeString} to ${newdf.schema.treeString}")
    newdf
  }

  /*
   * Convert IJoinedFlatTableDesc to SQL statement
   */
  def generateSelectDataStatement(flatDesc: IJoinedFlatTableDesc,
                                  singleLine: Boolean,
                                  skipAs: Array[String]): String = {
    val sep: String = {
      if (singleLine) " "
      else "\n"
    }
    val skipAsList = {
      if (skipAs == null) ListBuffer.empty[String]
      else skipAs.toList
    }
    val sql: StringBuilder = new StringBuilder
    sql.append("SELECT" + sep)
    for (i <- 0 until flatDesc.getAllColumns.size()) {
      val col: TblColRef = flatDesc.getAllColumns.get(i)
      sql.append(",")
      val colTotalName: String =
        String.format(Locale.ROOT, "%s.%s", col.getTableRef.getTableName, col.getName)
      if (skipAsList.contains(colTotalName)) {
        sql.append(col.getExpressionInSourceDB + sep)
      } else {
        sql.append(col.getExpressionInSourceDB + " as " + colName(col) + sep)
      }
    }
    appendJoinStatement(flatDesc, sql, singleLine)
    appendWhereStatement(flatDesc, sql, singleLine)
    sql.toString
  }

  def appendJoinStatement(flatDesc: IJoinedFlatTableDesc,
                          sql: StringBuilder,
                          singleLine: Boolean): Unit = {
    val sep: String =
      if (singleLine) " "
      else "\n"
    val dimTableCache: util.Set[TableRef] = Sets.newHashSet[TableRef]
    val model: DataModel = flatDesc.getDataModel
    val rootTable: TableRef = model.getRootFactTable
    sql.append(
      "FROM " + flatDesc.getDataModel.getRootFactTable.getTableIdentity + " as " + rootTable.getAlias + " " + sep)
    for (lookupDesc <- model.getJoinTables.asScala) {
      val join: JoinDesc = lookupDesc.getJoin
      if (join != null && join.getType == "" == false) {
        val joinType: String = join.getType.toUpperCase(Locale.ROOT)
        val dimTable: TableRef = lookupDesc.getTableRef
        if (!dimTableCache.contains(dimTable)) {
          val pk: Array[TblColRef] = join.getPrimaryKeyColumns
          val fk: Array[TblColRef] = join.getForeignKeyColumns
          if (pk.length != fk.length) {
            throw new RuntimeException(
              "Invalid join condition of lookup table:" + lookupDesc)
          }
          sql.append(
            joinType + " JOIN " + dimTable.getTableIdentity + " as " + dimTable.getAlias + sep)
          sql.append("ON ")
          var i: Int = 0
          while ( {
            i < pk.length
          }) {
            if (i > 0) sql.append(" AND ")
            sql.append(
              fk(i).getExpressionInSourceDB + " = " + pk(i).getExpressionInSourceDB)

            {
              i += 1
              i - 1
            }
          }
          sql.append(sep)
          dimTableCache.add(dimTable)
        }
      }
    }
  }

  private def appendWhereStatement(flatDesc: IJoinedFlatTableDesc,
                                   sql: StringBuilder,
                                   singleLine: Boolean): Unit = {
    val sep: String =
      if (singleLine) " "
      else "\n"
    val whereBuilder: StringBuilder = new StringBuilder
    whereBuilder.append("WHERE 1=1")
    val model: DataModel = flatDesc.getDataModel
    if (StringUtils.isNotEmpty(model.getFilterCondition)) {
      whereBuilder
        .append(" AND (")
        .append(model.getFilterCondition)
        .append(") ")
    }
    val partDesc: PartitionDesc = model.getPartitionDesc
    val segRange: SegmentRange[_ <: Comparable[_]] = flatDesc.getSegRange
    if (flatDesc.getSegment != null && partDesc != null
      && partDesc.getPartitionDateColumn != null && segRange != null && !segRange.isInfinite) {
      val builder =
        flatDesc.getDataModel.getPartitionDesc.getPartitionConditionBuilder
      if (builder != null) {
        whereBuilder.append(" AND (")
        whereBuilder.append(
          builder
            .buildDateRangeCondition(partDesc, flatDesc.getSegment, segRange))
        whereBuilder.append(")" + sep)
      }

      sql.append(whereBuilder.toString)
    }
  }

  def colName(col: TblColRef): String = {
    col.getTableAlias + "_" + col.getName
  }

}
