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
import org.apache.kylin.engine.spark.builder.v3dict.GlobalDictionaryBuilderHelper.{checkAnswer, genDataWithWrapEncodeCol, genRandomData}
import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.spark.sql.KapFunctions.dict_encode_v3
import org.apache.spark.sql.Row
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.apache.spark.sql.functions.{col, count, countDistinct}
import org.apache.spark.sql.types._

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class GlobalDictionarySuite extends SparderBaseFunSuite with LocalMetadata with SharedSparkSession {

  test("KE-35145 Test Continuously Build Dictionary") {
    val project = "p1"
    val tableName = "t1"
    val colName = "c1"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, tableName, colName, null)
    DeltaTable.createIfNotExists()
      .tableName("original_c1")
      .addColumn(encodeColName, StringType).execute()
    for (_ <- 0 until 10) {
      val originalDF = genRandomData(spark, encodeColName, 100, 1)
      val df = genDataWithWrapEncodeCol(encodeColName, originalDF)
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
        |SELECT count(DISTINCT t1_0_DOT_0_c1)
        |   FROM default.original_c1
      """.stripMargin)
    checkAnswer(originalDF, dictDF)
  }

  test("KE-35145 Test Concurrent Build Dictionary") {
    val project = "p1"
    val tableName = "t1"
    val colName = "c2"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, tableName, colName, null)
    val pool = Executors.newFixedThreadPool(10)
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(pool)

    DeltaTable.createIfNotExists()
      .tableName("original_c2")
      .addColumn(encodeColName, StringType).execute()

    val buildDictTask = new Runnable {
      override def run(): Unit = {
        val originalDF = genRandomData(spark, encodeColName, 100, 1)
        val dictDF = genDataWithWrapEncodeCol(encodeColName, originalDF)
        DeltaTable.forName("original_c2")
          .merge(originalDF, "1 != 1")
          .whenNotMatched()
          .insertAll()
          .execute()
        DictionaryBuilder.buildGlobalDict(project, spark, dictDF.queryExecution.analyzed)
      }
    }

    for (_ <- 0 until 10) {ec.submit(buildDictTask)}
    ec.awaitTermination(2, TimeUnit.MINUTES)

    val originalDF = spark.sql(
      """
        |SELECT count(DISTINCT t1_0_DOT_0_c2)
        |   FROM default.original_c2
      """.stripMargin)

    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-35145 Test the v3 dictionary with random data") {
    val project = "p1"
    val tableName = "t1"
    val colName = "c3"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, tableName, colName, null)
    val df = genRandomData(spark, encodeColName, 1000, 2)
    val dictDF = genDataWithWrapEncodeCol(encodeColName, df)
    DictionaryBuilder.buildGlobalDict(project, spark, dictDF.queryExecution.analyzed)

    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }

  test("KE-35145 With null dict value") {
    val project = "p1"
    val tableName = "t1"
    val colName = "c4"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, tableName, colName, null)
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

    val dictCol = Seq(dict_encode_v3(col(encodeColName)).alias(colName + "_KE_ENCODE"))

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
    val tableName = "t1"
    val colName = "c5"
    val encodeColName: String = tableName + NSparkCubingUtil.SEPARATOR + colName
    val context = new DictionaryContext(project, tableName, colName, null)
    var schema = new StructType
    schema = schema.add(encodeColName, StringType)
    val data = Seq.empty[Row]

    val dictCol = Seq(dict_encode_v3(col(encodeColName)).alias(encodeColName + "_KE_ENCODE"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    val dictDfPlan = df
      .select(df.schema.map(ty => col(ty.name)) ++ dictCol: _*)
      .queryExecution
      .analyzed
    DictionaryBuilder.buildGlobalDict("p2", spark, dictDfPlan)
    val originalDF = df.agg(countDistinct(encodeColName))
    val dictPath: String = DictionaryBuilder.getDictionaryPath(context)
    val dictResultDF = DeltaTable.forPath(dictPath).toDF.agg(count(col("dict_key")))
    checkAnswer(originalDF, dictResultDF)
  }
}
