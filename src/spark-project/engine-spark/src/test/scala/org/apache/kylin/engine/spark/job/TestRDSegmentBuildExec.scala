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

import com.google.common.collect.Lists
import org.apache.kylin.common.util.TestUtils.getTestConfig
import org.apache.kylin.common.{KapConfig, KylinConfig}
import org.apache.kylin.engine.spark.job.stage.BuildParam
import org.apache.kylin.metadata.cube.model._
import org.apache.kylin.metadata.model.{NTableMetadataManager, SegmentRange, TableDesc, TableExtDesc}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.junit.jupiter.api.Assertions.assertEquals
import org.mockito.Mockito
import org.scalatest.PrivateMethodTester

import java.util
import scala.collection.JavaConverters._

class TestRDSegmentBuildExec extends SparderBaseFunSuite with PrivateMethodTester
  with AdaptiveSparkPlanHelper with SharedSparkSession with LocalMetadata {
  private val PROJECT = "infer_filter"
  private val MODEL_NAME1 = "89af4ee2-2cdb-4b07-b39e-4c29856309ab"

  test("test evaluateColumnTotalFromTableDesc") {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, PROJECT)
    val df: NDataflow = dsMgr.getDataflow(MODEL_NAME1)
    // cleanup all segments first
    val update = new NDataflowUpdate(df.getUuid)
    update.setToRemoveSegsWithArray(df.getSegments.asScala.toArray)
    dsMgr.updateDataflow(update)

    val seg = dsMgr.appendSegment(df, new SegmentRange.TimePartitionedSegmentRange(0L, 1356019200000L))
    val segmentJob = Mockito.mock(classOf[SegmentJob])
    Mockito.when(segmentJob.getConfig).thenReturn(getTestConfig)
    Mockito.when(segmentJob.getSparkSession).thenReturn(spark)
    val buildParam = new BuildParam()
    val exec = new RDSegmentBuildExec(segmentJob, seg, buildParam)
    exec.initFlatTableOnDetectResource()

    val tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig, PROJECT)
    var result = exec.evaluateColumnTotalFromTableDesc(tableMetadataManager, 0, "", "UUID")
    assertEquals(0, result)

    val flatTableDesc = buildParam.getFlatTableDesc
    val columns = flatTableDesc.getColumns.asScala.toArray
    var totalRows = 0
    val columnsIdentity = columns.map(col => col.getIdentity)
    columns.map(colRef => tableMetadataManager.getTableDesc(flatTableDesc.getTableName(colRef.toString)))
      .filter(tableMetadataDesc => tableMetadataDesc != null).distinct.foreach(tableMetadataDesc => {
      val tableName = tableMetadataDesc.getName
      mockTableStats(tableMetadataDesc, totalRows)
      if (totalRows % 2 != 0) {
        val baseDir = KapConfig.getInstanceFromEnv.getMetadataWorkingDirectory
        val tmp = spark.range(totalRows)
        tmp.write.mode(SaveMode.Overwrite).parquet(baseDir + tableName + ".parquet")
        tableMetadataDesc.setLastSnapshotPath(tableName + ".parquet")
        tableMetadataDesc.setLastSnapshotSize(totalRows)
      }
      val colRef = tableMetadataDesc.getColumns.find(columnDesc => columnsIdentity.contains(columnDesc.getIdentity)).get
      val colIdentity = tableMetadataDesc.getIdentity + "." + colRef.getName
      result = exec.evaluateColumnTotalFromTableDesc(tableMetadataManager, 0, tableMetadataDesc.getName, colIdentity)
      assertEquals(totalRows, result)
      totalRows += 1
    })
  }

  private def mockTableStats(tableDesc: TableDesc, totalRows: Int): TableExtDesc = {
    val tableMetadataManager: NTableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv, PROJECT)
    var tableExt: TableExtDesc = tableMetadataManager.getOrCreateTableExt(tableDesc)
    tableExt = tableMetadataManager.copyForWrite(tableExt)
    val columnStats: util.List[TableExtDesc.ColumnStats] = Lists.newArrayList[TableExtDesc.ColumnStats]
    for (columnDesc <- tableDesc.getColumns) {
      if (!columnDesc.isComputedColumn) {
        var colStats: TableExtDesc.ColumnStats = tableExt.getColumnStatsByName(columnDesc.getName)
        if (colStats == null) {
          colStats = new TableExtDesc.ColumnStats
          colStats.setColumnName(columnDesc.getName)
        }
        if ("CAL_DT" == columnDesc.getName) colStats.setCardinality(1000)
        else if ("TRANS_ID" == columnDesc.getName) colStats.setCardinality(10000)
        else colStats.setCardinality(100)
        columnStats.add(colStats)
      }
    }
    if (totalRows % 2 == 0) {
      tableExt.setTotalRows(totalRows)
    }

    tableExt.setColumnStats(columnStats)
    tableMetadataManager.saveTableExt(tableExt)
    tableExt
  }
}
