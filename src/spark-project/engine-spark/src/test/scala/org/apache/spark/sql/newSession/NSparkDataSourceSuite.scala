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
package org.apache.spark.sql.newSession

import java.util.Locale
import com.google.common.collect.Maps
import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource
import org.apache.kylin.engine.spark.builder.CreateFlatTable
import org.apache.kylin.engine.spark.source.NSparkDataSource
import org.apache.kylin.metadata.cube.model.{NCubeJoinedFlatTableDesc, NDataflowManager}
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.model.SegmentRange
import org.apache.kylin.source.SourceFactory
import org.apache.kylin.engine.spark.job.KylinBuildEnv
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.SparderTypeUtil

import scala.collection.JavaConverters._


/**
 * Equivalence [[org.apache.kylin.engine.spark.mockup.CsvSourceTest]]
 */
class NSparkDataSourceSuite extends SQLTestUtils with WithKylinExternalCatalog {

  private val DEFAULT_TABLE = "DEFAULT.TEST_KYLIN_FACT"
  private val project = "default"
  private val database = "default"

  test("testSourceMetadataExplorer") {
    val sparkCsvSource = new NSparkDataSource(null)
    val metaSource = sparkCsvSource.getSourceMetadataExplorer
    val databases = metaSource.listDatabases.asScala
    assert(databases.size >= 3) // at least 3
    assert(databases.contains(database.toUpperCase(Locale.ROOT)))
    val tables = metaSource.listTables(database.toUpperCase(Locale.ROOT)).asScala.map(_.toUpperCase(Locale.ROOT))
    val table = DEFAULT_TABLE.split("\\.")(1)
    assert(tables.contains(table))

    val tableDescTableExtDescPair = metaSource.loadTableMetadata(database, table, project)
    val tableDesc = tableDescTableExtDescPair.getFirst
    val readableTable = sparkCsvSource.createReadableTable(tableDesc)
    assert(readableTable.exists())
  }

  test("testGetTable") {
    overwriteSystemProp("kylin.external.catalog.mockup.sleep-interval", "1s")
    // make sure KylinBuildEnv not be null
    val kylinBuildEnv = KylinBuildEnv.getOrCreate(kylinConf)
    val tableMgr = NTableMetadataManager.getInstance(kylinConf, project)
    val fact = tableMgr.getTableDesc(DEFAULT_TABLE)
    val colDescs = fact.getColumns
    val sparkCsvSource = new NSparkDataSource(null)
    val cubingSource = sparkCsvSource.adaptToBuildEngine(classOf[NSparkCubingSource])
    val df = cubingSource.getSourceData(fact, spark, Maps.newHashMap[String, String]())
    assertResult(10)(df.take(10).length)
    val schema = df.schema
    for (i <- 0 until colDescs.length) {
      val field = schema.fields(i)
      assertResult(colDescs(i).getName)(field.name)
      assertResult(SparderTypeUtil.toSparkType(colDescs(i).getType))(field.dataType)
    }
  }

  test("testGetSegmentRange") {
    val sparkCsvSource = new NSparkDataSource(null)
    val segmentRange = sparkCsvSource.getSegmentRange("0", "21423423")
    assert(segmentRange.isInstanceOf[SegmentRange.TimePartitionedSegmentRange])
    assertResult(0L)(segmentRange.getStart)
    assertResult(21423423L)(segmentRange.getEnd)

    val segmentRange2 = sparkCsvSource.getSegmentRange("", "")
    assert(segmentRange2.isInstanceOf[SegmentRange.TimePartitionedSegmentRange])
    assertResult(0L)(segmentRange2.getStart)
    assertResult(Long.MaxValue)(segmentRange2.getEnd)
  }

  test("testGetFlatTable") {
    log.info(kylinConf.getMetadataUrl.toString)
    // make sure KylinBuildEnv not be null,
    val kylinBuildEnv = KylinBuildEnv.getOrCreate(kylinConf)
    val dsMgr = NDataflowManager.getInstance(kylinConf, project)
    val dataflow = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa")
    val model = dataflow.getModel
    assertResult(classOf[NSparkDataSource])(SourceFactory.getSource(model.getRootFactTable.getTableDesc).getClass)

    val flatTableDesc =
      new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan,
        new SegmentRange.TimePartitionedSegmentRange(0L, System.currentTimeMillis), true)

    val flatTable = new CreateFlatTable(flatTableDesc, null, null, spark, null)
    val ds = flatTable.generateDataset(needEncode = false, needJoin = true)

    assertResult(10)(ds.take(10).length)

    ds.schema
      .foreach(field => assert(null != model.findColumn(model.getColumnNameByColumnId(Integer.parseInt(field.name)))))

    val cols = dataflow.getIndexPlan.getEffectiveDimCols.keySet.asScala.toList
      .map(id => new ColumnName(String.valueOf(id)))
    assertResult(10)(ds.select(cols: _*).take(10).length)
  }
}
