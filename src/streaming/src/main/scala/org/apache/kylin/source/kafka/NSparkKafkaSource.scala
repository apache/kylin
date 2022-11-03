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
package org.apache.kylin.source.kafka

import java.util.Map
import java.util.concurrent.ArrayBlockingQueue

import org.apache.commons.lang.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.NSparkCubingEngine
import org.apache.kylin.metadata.model.{IBuildable, SegmentRange, TableDesc}
import org.apache.kylin.source.{IReadableTable, ISampleDataDeployer, ISource, ISourceMetadataExplorer}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source


class NSparkKafkaSource(val kylinConfig: KylinConfig) extends ISource {
  private val textFileQueue = new ArrayBlockingQueue[String](1000)
  private var msEvents: MemoryStream[Row] = null
  private var memoryStreamEnabled = false

  // scalastyle:off

  /**
   * Return an explorer to sync table metadata from the data source.
   */
  override def getSourceMetadataExplorer: ISourceMetadataExplorer = {
    new KafkaExplorer()
  }

  /**
   * Return an adaptor that implements specified interface as requested by the build engine.
   * The IMRInput in particular, is required by the MR build engine.
   */
  override def adaptToBuildEngine[I](engineInterface: Class[I]): I = {
    if (engineInterface eq classOf[NSparkCubingEngine.NSparkCubingSource]) {
      val source = new NSparkCubingEngine.NSparkCubingSource() {
        override def getSourceData(table: TableDesc, ss: SparkSession, parameters: Map[String, String]): Dataset[Row] = {
          if (KylinConfig.getInstanceFromEnv.isUTEnv) {
            val schema = new StructType().add("value", StringType)
            if (memoryStreamEnabled) {
              msEvents = MemoryStream[Row](1)(RowEncoder(schema), ss.sqlContext)
              val text = Source.fromFile(textFileQueue.take())
              msEvents.addData(Row(text.getLines().mkString))
              msEvents.toDS()
            } else {
              val rdd = ss.read.text(textFileQueue.take()).rdd
              val dataframe = ss.createDataFrame(rdd, schema)
              dataframe.as[Row](RowEncoder(schema))
            }
          } else {
            ss.readStream.format("kafka").options(parameters).load()
          }
        }
      }
      return source.asInstanceOf[I]
    }
    throw new IllegalArgumentException("Unsupported engine interface: " + engineInterface)
  }

  /**
   * Return a ReadableTable that can iterate through the rows of given table.
   */
  override def createReadableTable(tableDesc: TableDesc): IReadableTable = {
    throw new UnsupportedOperationException
  }

  /**
   * Give the source a chance to enrich a SourcePartition before build start.
   * Particularly, Kafka source use this chance to define start/end offsets within each partition.
   */
  override def enrichSourcePartitionBeforeBuild(buildable: IBuildable, segmentRange:
  SegmentRange[_ <: Comparable[_]]): SegmentRange[_ <: Comparable[_]] = {
    throw new UnsupportedOperationException
  }

  /**
   * Return an object that is responsible for deploying sample (CSV) data to the source database.
   * For testing purpose.
   */
  override def getSampleDataDeployer: ISampleDataDeployer = {
    throw new UnsupportedOperationException
  }

  override def getSegmentRange(start: String, end: String): SegmentRange[_ <: Comparable[_]] = {
    var startTime = start
    var endTime = end
    if (StringUtils.isEmpty(start)) startTime = "0"
    if (StringUtils.isEmpty(end)) endTime = "" + Long.MaxValue
    new SegmentRange.KafkaOffsetPartitionedSegmentRange(startTime.toLong, endTime.toLong)
  }

  def enableMemoryStream(): Boolean = {
    this.memoryStreamEnabled
  }

  def enableMemoryStream(mse: Boolean): Unit = {
    this.memoryStreamEnabled = mse
  }

  def post(textFile: String): Unit = {
    if (msEvents != null && memoryStreamEnabled) {
      val text = Source.fromFile(textFile)
      msEvents.addData(Row(text.getLines().mkString))
    } else {
      textFileQueue.offer(textFile)
    }
  }

  override def supportBuildSnapShotByPartition = true

  // scalastyle:on
}
