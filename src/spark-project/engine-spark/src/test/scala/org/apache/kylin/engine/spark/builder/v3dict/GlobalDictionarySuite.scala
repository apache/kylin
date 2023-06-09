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
import org.apache.hadoop.fs.Path
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.exception.KylinRuntimeException
import org.apache.kylin.common.util.HadoopUtil
import org.apache.kylin.engine.spark.builder.v3dict.GlobalDictionaryBuilderHelper.{checkAnswer, genDataWithWrapEncodeCol, genRandomData}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.spark.sql.KapFunctions.dict_encode_v3
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.functions.{col, count, countDistinct}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.util.SerializableConfiguration
import org.mockito.Mockito.{spy, when}

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class GlobalDictionarySuite extends SparderBaseFunSuite with LocalMetadata with SharedSparkSession {

  private var pool: ExecutorService = Executors.newFixedThreadPool(10)
  implicit var ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(pool)

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    if (pool.isShutdown) {
      pool = Executors.newFixedThreadPool(10)
      ec = ExecutionContext.fromExecutorService(pool)
    }
  }

  test("KE-35145 Test Continuously Build Dictionary") {
    val project = "p1"
    val dbName = "db1"
    val tableName = "t2"
    val colName = "c1"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    DeltaTable.createIfNotExists()
      .tableName("original_c1")
      .addColumn(encodeColName, StringType).execute()
    for (_ <- 0 until 10) {
      val originalDF = genRandomData(spark, encodeColName, 100, 1)
      val df = genDataWithWrapEncodeCol(dbName, encodeColName, originalDF)
      DeltaTable.forName("original_c1")
        .merge(originalDF, "1 != 1")
        .whenNotMatched()
        .insertAll()
        .execute()
      Future.successful(DictionaryBuilder.buildGlobalDict(project, spark, df.queryExecution.analyzed))
    }

    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    val originalDF = spark.sql(
      """
        |SELECT count(DISTINCT t2_0_DOT_0_c1)
        |   FROM default.original_c1
      """.stripMargin)
    checkAnswer(originalDF, dictDF)
  }

  test("KE-35145 Test Concurrent Build Dictionary") {
    val project = "p1"
    val dbName = "db1"
    val tableName = "t1"
    val colName = "c2"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, dbName, tableName, colName, null)

    DeltaTable.createIfNotExists()
      .tableName("original")
      .addColumn(encodeColName, StringType).execute()

    val buildDictTask = genBuildDictTask(spark, context)

    for (_ <- 0 until 10) {
      ec.submit(buildDictTask)
    }
    ec.shutdown()
    ec.awaitTermination(2, TimeUnit.MINUTES)

    val originalDF = spark.sql(
      """
        |SELECT count(DISTINCT t1_0_DOT_0_c2)
        |   FROM default.original
      """.stripMargin)

    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-35145 Test the v3 dictionary with random data") {
    val project = "p1"
    val dbName = "db1"
    val tableName = "t1"
    val colName = "c3"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    val df = genRandomData(spark, encodeColName, 1000, 2)
    val dictDF = genDataWithWrapEncodeCol(dbName, encodeColName, df)
    DictionaryBuilder.buildGlobalDict(project, spark, dictDF.queryExecution.analyzed)

    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-35145 With null dict value") {
    val project = "p1"
    val dbName = "db1"
    val tableName = "t1"
    val colName = "c4"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    var schema = new StructType
    schema = schema.add(encodeColName, StringType)
    val data = Seq(
      Row("a"),
      Row("b"),
      Row("a"),
      Row("b"),
      Row("a"),
      Row("b"),
      Row("a"),
      Row("null"),
      Row("null"),
      Row("null"),
      Row("b"),
      Row("a"),
      Row("b"))

    val dictCol = Seq(dict_encode_v3(col(encodeColName), dbName).alias(colName + "_KYLIN_ENCODE"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val dictDfPlan = df
      .select(df.schema.map(ty => col(ty.name)) ++ dictCol: _*)
      .queryExecution
      .analyzed
    DictionaryBuilder.buildGlobalDict("p1", spark, dictDfPlan)
    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-35145 Build dict with null value") {
    val project = "p1"
    val dbName = "db1"
    val tableName = "t1"
    val colName = "c5"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    var schema = new StructType
    schema = schema.add(encodeColName, StringType)
    val data = Seq.empty[Row]

    val dictCol = Seq(dict_encode_v3(col(encodeColName), dbName).alias(encodeColName + "_KYLIN_ENCODE"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val dictDfPlan = df
      .select(df.schema.map(ty => col(ty.name)) ++ dictCol: _*)
      .queryExecution
      .analyzed
    DictionaryBuilder.buildGlobalDict(project, spark, dictDfPlan)
    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-41744 Optimize the dict files to avoid too many small files") {
    overwriteSystemProp("kylin.build.v3dict-file-num-limit", "5")
    overwriteSystemProp("kylin.build.v3dict-file-retention", "0h")
    val project = "p1"
    val dbName = "db1"
    val tableName = "t1"
    val colName = "c2"

    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    DeltaTable.createIfNotExists()
      .tableName("original")
      .addColumn(encodeColName, StringType).execute()

    val buildDictTask = genBuildDictTask(spark, context)

    for (_ <- 0 until 11) {
      ec.execute(buildDictTask)
    }
    ec.shutdown()
    ec.awaitTermination(10, TimeUnit.MINUTES)

    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val deltaLog = DeltaLog.forTable(spark, dictPath)
    val numOfFiles = deltaLog.snapshot.numOfFiles
    logInfo(s"Dict file num $numOfFiles")
    assert(numOfFiles <= KylinConfig.getInstanceFromEnv.getV3DictFileNumLimit)

    val numFileRemaining = DeltaFileOperations.recursiveListDirs(
      spark,
      Seq(dictPath),
      spark.sparkContext.broadcast(new SerializableConfiguration(deltaLog.newDeltaHadoopConf()))
    ).count()
    assert(numFileRemaining < numOfFiles + deltaLog.snapshot.numOfRemoves)
  }
  test("KE-41980 Test failure to initialize dictionary file") {
    val project = "project"
    val dbName = "db"
    val tableName = "table"
    val colName = "col"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName

    val context = new DictionaryContext(project, dbName, tableName, colName, null)
    val dictPath = DictionaryBuilder.getDictionaryPath(context)
    val dictDF = spy(genRandomData(spark, encodeColName, 10, 2))

    // mock write delta table failed and throw a Exception
    when(dictDF.write.mode(SaveMode.Overwrite).format("delta").save(dictPath))
      .thenThrow(new RuntimeException())

    intercept[RuntimeException] {
      DictionaryBuilder.initAndSaveDict(dictDF, context)
    }

    // after mock throw RuntimeException, dictPath will be delete
    assert(HadoopUtil.getWorkingFileSystem().exists(new Path(dictPath)) == false)

    // write temp file to dictPath, which is not a delta table
    // so that getDictionaryPathAndCheck will throw KylinRuntimeException
    HadoopUtil.writeStringToHdfs("tempString4Test", new Path(dictPath))
    intercept[KylinRuntimeException] {
      DictionaryBuilder.getDictionaryPathAndCheck(context)
    }
  }

  def genBuildDictTask(spark: SparkSession, context: DictionaryContext): Runnable = {
    new Runnable {
      override def run(): Unit = {
        val encodeColName: String = context.tableName + NSparkCubingUtil.SEPARATOR + context.columnName
        val originalDF = genRandomData(spark, encodeColName, 100, 1)
        val dictDF = genDataWithWrapEncodeCol(context.dbName, encodeColName, originalDF)
        DeltaTable.forName("original")
          .merge(originalDF, "1 != 1")
          .whenNotMatched()
          .insertAll()
          .execute()
        DictionaryBuilder.buildGlobalDict(context.project, spark, dictDF.queryExecution.analyzed)
      }
    }
  }
}
