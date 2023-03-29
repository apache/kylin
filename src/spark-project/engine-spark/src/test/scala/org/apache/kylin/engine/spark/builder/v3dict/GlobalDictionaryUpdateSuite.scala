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

package org.apache.kylin.engine.spark.builder.v3dict

import io.delta.tables.DeltaTable
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.builder.v3dict.GlobalDictionaryBuilderHelper.{genDataWithWrapEncodeCol, genRandomData}
import org.apache.kylin.engine.spark.builder.{DFDictionaryBuilder, DictionaryBuilderHelper}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory
import org.apache.kylin.metadata.cube.model.{NDataSegment, NDataflow, NDataflowManager}
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.dict.{NGlobalDictMetaInfo, NGlobalDictionaryV2}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.{col, count, countDistinct}
import org.apache.spark.sql.{Dataset, Row}
import org.junit.Assert

import java.util

class GlobalDictionaryUpdateSuite extends SparderBaseFunSuite with LocalMetadata with SharedSparkSession {

  private val DEFAULT_PROJECT = "default"
  private val CUBE_NAME = "89af4ee2-2cdb-4b07-b39e-4c29856309aa"

  def getTestConfig: KylinConfig = {
    val config = KylinConfig.getInstanceFromEnv
    config
  }

  test("KE-35145 Global Dict Upgrade v3") {
    getTestConfig.setProperty("kylin.build.is-v3dict-enable", "true")
    getTestConfig.setProperty("kylin.build.is-v2dict-enable", "false")
    getTestConfig.setProperty("kylin.build.is-convert-v3dict-enable", "true")
    val dictCol = buildV2Dict()
    buildV3Dict(dictCol)
  }

  private def buildV2Dict() = {
    val dsMgr: NDataflowManager = NDataflowManager.getInstance(getTestConfig, DEFAULT_PROJECT)
    val df: NDataflow = dsMgr.getDataflow(CUBE_NAME)
    val seg = df.getLastSegment
    val nSpanningTree = NSpanningTreeFactory.fromLayouts(seg.getIndexPlan.getAllLayouts, df.getUuid)
    val dictColSet = DictionaryBuilderHelper.extractTreeRelatedGlobalDicts(seg, nSpanningTree.getAllIndexEntities)

    val dictCol = dictColSet.iterator().next()
    val encodeColName: String = StringUtils.split(dictCol.getTable, ".").apply(1) + NSparkCubingUtil.SEPARATOR + dictCol.getName
    val randomDF = genRandomData(spark, encodeColName, 1000, 10)
    val meta1 = prepareV2Dict(seg, randomDF, dictColSet)
    Assert.assertEquals(1000, meta1.getDictCount)
    dictCol
  }

  private def buildV3Dict(dictCol: TblColRef): Unit = {
    val tableName = StringUtils.split(dictCol.getTable, ".").apply(1)
    val dbName = dictCol.getTableRef.getTableDesc.getDatabase
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + dictCol.getName
    val context = new DictionaryContext(DEFAULT_PROJECT, dbName, tableName, dictCol.getName, null)
    val df = genRandomData(spark, encodeColName, 100, 10)
    val dictDF = genDataWithWrapEncodeCol(dbName, encodeColName, df)
    DictionaryBuilder.buildGlobalDict(DEFAULT_PROJECT, spark, dictDF.queryExecution.analyzed)

    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    Assert.assertEquals(originalDF.head().getLong(0) + 1000, dictResultDF.head().getLong(0))
  }

  def prepareV2Dict(seg: NDataSegment, randomDataSet: Dataset[Row], dictColSet: util.Set[TblColRef]): NGlobalDictMetaInfo = {
    val dictionaryBuilder = new DFDictionaryBuilder(randomDataSet, seg, randomDataSet.sparkSession, dictColSet)
    val colName = dictColSet.iterator().next()
    val bucketPartitionSize = DictionaryBuilderHelper.calculateBucketSize(seg, colName, randomDataSet)
    dictionaryBuilder.build(colName, bucketPartitionSize, randomDataSet)
    val dict = new NGlobalDictionaryV2(seg.getProject,
      colName.getTable,
      colName.getName,
      seg.getConfig.getHdfsWorkingDirectory)
    dict.getMetaInfo
  }
}
