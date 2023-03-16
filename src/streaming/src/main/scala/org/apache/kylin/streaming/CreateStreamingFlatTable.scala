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

package org.apache.kylin.streaming

import java.nio.ByteBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.NSparkCubingEngine
import org.apache.kylin.engine.spark.builder.CreateFlatTable
import org.apache.kylin.engine.spark.job.{FlatTableHelper, NSparkCubingUtil}
import org.apache.kylin.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataSegment}
import org.apache.kylin.metadata.cube.utils.StreamingUtils
import org.apache.kylin.metadata.model._
import org.apache.kylin.parser.AbstractDataParser
import org.apache.kylin.source.SourceFactory
import org.apache.kylin.streaming.common.CreateFlatTableEntry
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.kylin.guava30.shaded.common.base.Preconditions

class CreateStreamingFlatTable(entry: CreateFlatTableEntry) extends
  CreateFlatTable(entry.flatTable, entry.seg, entry.toBuildTree, entry.ss, entry.sourceInfo) {

  import org.apache.kylin.engine.spark.builder.CreateFlatTable._

  private val MAX_OFFSETS_PER_TRIGGER = "maxOffsetsPerTrigger"
  private val STARTING_OFFSETS = "startingOffsets"
  private val SECURITY_PROTOCOL = "security.protocol"
  private val SASL_MECHANISM = "sasl.mechanism"

  var lookupTablesGlobal: mutable.LinkedHashMap[JoinTableDesc, Dataset[Row]] = _
  var factTableDataset: Dataset[Row] = _
  var tableRefreshInterval: Long = -1L

  def generateStreamingDataset(config: KylinConfig): Dataset[Row] = {
    val model = flatTable.getDataModel
    val tableDesc = model.getRootFactTable.getTableDesc
    val kafkaParam = tableDesc.getKafkaConfig.getKafkaParam
    val kafkaJobParams = config.getStreamingKafkaConfigOverride.asScala
    val securityProtocol = kafkaJobParams.get(SECURITY_PROTOCOL)
    if (securityProtocol.isDefined) {
      kafkaJobParams.remove(SECURITY_PROTOCOL)
      kafkaJobParams.put("kafka." + SECURITY_PROTOCOL, securityProtocol.get)
    }
    val saslMechanism = kafkaJobParams.get(SASL_MECHANISM)
    if (saslMechanism.isDefined) {
      kafkaJobParams.remove(SASL_MECHANISM)
      kafkaJobParams.put("kafka." + SASL_MECHANISM, saslMechanism.get)
    }
    kafkaJobParams.foreach { param =>
      param._1 match {
        case MAX_OFFSETS_PER_TRIGGER => if (param._2.toInt > 0) {
          val maxOffsetsPerTrigger = param._2.toInt
          kafkaParam.put("maxOffsetsPerTrigger", String.valueOf(maxOffsetsPerTrigger))
        }
        case STARTING_OFFSETS => if (!StringUtils.isEmpty(param._2)) {
          kafkaParam.put("startingOffsets", param._2)
        }
        case _ => kafkaParam.put(param._1, param._2)
      }
    }

    val originFactTable = SourceFactory
      .createEngineAdapter(tableDesc, classOf[NSparkCubingEngine.NSparkCubingSource])
      .getSourceData(tableDesc, ss, kafkaParam)

    val schema =
      StructType(
        tableDesc.getColumns.map { columnDescs =>
          StructField(columnDescs.getName, SparderTypeUtil.toSparkType(columnDescs.getType))
        }
      )
    val rootFactTable = changeSchemaToAliasDotName(
      CreateStreamingFlatTable.castDF(originFactTable, schema, partitionColumn(), entry.parserName).alias(model.getRootFactTable.getAlias),
      model.getRootFactTable.getAlias)

    factTableDataset =
      if (!StringUtils.isEmpty(entry.watermark)) {
        import org.apache.spark.sql.functions._
        val cols = model.getRootFactTable.getColumns.asScala.map(item => {
          col(NSparkCubingUtil.convertFromDot(item.getBackTickIdentity))
        }).toList
        rootFactTable.withWatermark(partitionColumn(), entry.watermark).groupBy(cols: _*).count()
      } else {
        rootFactTable
      }
    tableRefreshInterval = StreamingUtils.parseTableRefreshInterval(config.getStreamingTableRefreshInterval)
    loadLookupTables()
    joinFactTableWithLookupTables(factTableDataset, lookupTablesGlobal, model, ss)
  }

  def loadLookupTables(): Unit = {
    val ccCols = model().getRootFactTable.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    val cleanLookupCC = cleanComputColumn(ccCols.toSeq, factTableDataset.columns.toSet)
    lookupTablesGlobal = generateLookupTableDataset(model(), cleanLookupCC, ss)
    lookupTablesGlobal.foreach { case (_, df) =>
      df.persist(StorageLevel.MEMORY_AND_DISK)
    }
  }

  def shouldRefreshTable(): Boolean = {
    tableRefreshInterval > 0
  }

  def model(): NDataModel = {
    flatTable.getDataModel
  }

  def partitionColumn(): String = {
    val partitionDateTableDotColumn = model().getPartitionDesc.getPartitionDateColumn
    val dotIdx = partitionDateTableDotColumn.lastIndexOf(".")
    Preconditions.checkArgument(dotIdx != -1)
    partitionDateTableDotColumn.substring(dotIdx + 1)
  }

  def encodeStreamingDataset(seg: NDataSegment, model: NDataModel, batchDataset: Dataset[Row]): Dataset[Row] = {
    val ccCols = model.getRootFactTable.getColumns.asScala.toSet
    val (dictCols, encodeCols): GlobalDictType = assemblyGlobalDictTuple(seg, toBuildTree)
    val encodedDataset = encodeWithCols(batchDataset, ccCols, dictCols, encodeCols)
    val filterEncodedDataset = FlatTableHelper.applyFilterCondition(flatTable, encodedDataset, needReplaceDot = true)

    flatTable match {
      case joined: NCubeJoinedFlatTableDesc =>
        changeSchemeToColumnIndice(filterEncodedDataset, joined)
      case unsupported =>
        throw new UnsupportedOperationException(
          s"Unsupported flat table desc type : ${unsupported.getClass}.")
    }
  }
}

object CreateStreamingFlatTable {
  def apply(createFlatTableEntry: CreateFlatTableEntry): CreateStreamingFlatTable = {
    new CreateStreamingFlatTable(createFlatTableEntry)
  }

  def castDF(df: DataFrame, parsedSchema: StructType, partitionColumn: String, parserName: String): DataFrame = {
    df.selectExpr("CAST(value AS STRING) as rawValue")
      .mapPartitions { rows =>
        val dataParser: AbstractDataParser[ByteBuffer] = AbstractDataParser
          .getDataParser(parserName, Thread.currentThread.getContextClassLoader)

        val newRows = new PartitionRowIterator(rows, parsedSchema, partitionColumn, dataParser)
        newRows.filter(row => row.size == parsedSchema.length)
      }(RowEncoder(parsedSchema))
  }
}
