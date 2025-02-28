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

package org.apache.kylin.engine.spark.job.step.merge.partition

import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.TestUtils.getTestConfig
import org.apache.kylin.engine.spark.job.SegmentJob
import org.apache.kylin.metadata.cube.model.{NDataSegment, NDataflowManager}
import org.apache.kylin.metadata.model.NDataModel
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.Assert
import org.mockito.Mockito

class PartitionMergeColumnBytesTest extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

  val PROJECT = "default"
  val DATAFLOW_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"
  val SEGMENT_ID = "ef5e0663-feba-4ed2-b71c-21958122bbff"

  test("test PartitionMergeColumnBytes getStageName") {
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val dataModel = Mockito.mock(classOf[NDataModel])

    Mockito.when(dataSegment.getModel).thenReturn(dataModel)

    val partitionMergeColumnBytes = new PartitionMergeColumnBytes(segmentJob, dataSegment)
    Assert.assertEquals("PartitionMergeColumnBytes", partitionMergeColumnBytes.getStageName)
  }

  test("test PartitionMergeColumnBytes execute with multi partition filter range info enabled") {
    val config = getTestConfig
    config.setProperty("kylin.build.multi-partition-filter-enabled", "true")

    val jobContext = Mockito.mock(classOf[SegmentJob])
    val dataSegment = Mockito.mock(classOf[NDataSegment])
    val dataModel = Mockito.mock(classOf[NDataModel])

    Mockito.when(jobContext.getConfig).thenReturn(config)
    Mockito.when(jobContext.getSparkSession).thenReturn(spark)
    Mockito.when(jobContext.getDataflowId).thenReturn(DATAFLOW_ID)

    Mockito.when(dataSegment.getModel).thenReturn(dataModel)
    Mockito.when(dataSegment.getProject).thenReturn(PROJECT)
    Mockito.when(dataSegment.getId).thenReturn(SEGMENT_ID)

    // Mock build multi partition
    val dataflow = NDataflowManager.getInstance(config, PROJECT).getDataflow(DATAFLOW_ID)
    val mergedSegment = dataflow.getSegment(SEGMENT_ID)
    mergedSegment.setFlatTableReady(true)
    prepareFlatTableParquet(config)

    val mergeColumnBytes = new PartitionMergeColumnBytes(jobContext, dataSegment)
    Assert.assertEquals("PartitionMergeColumnBytes", mergeColumnBytes.getStageName)
    try {
      mergeColumnBytes.execute()
    } catch {
      case e: Exception => Assert.assertEquals("java.util.NoSuchElementException: next on empty iterator", e.getCause.toString)
    }
  }

  def prepareFlatTableParquet(config: KylinConfig): Unit = {
    val schema = StructType(List(
      StructField("col1", DataTypes.LongType, nullable = false),
      StructField("col2", DataTypes.LongType, nullable = false),
      StructField("col3", DataTypes.LongType, nullable = false),
      StructField("col4", DataTypes.StringType, nullable = false),
      StructField("col5", DataTypes.LongType, nullable = false)
    ))
    val rows = Seq(
      Row(1L, 5L, 1L, "Customer#000000001", 1L),
      Row(2L, 6L, 2L, "Customer#000000002", 2L)
    )
    val flatTableDS = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    flatTableDS.write.mode(SaveMode.Overwrite).parquet(config.getFlatTableDir(PROJECT, DATAFLOW_ID, SEGMENT_ID).toString)
  }

}
