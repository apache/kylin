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

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.KapFunctions.dict_encode_v3
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object GlobalDictionaryBuilderHelper {

  def checkAnswer(originalDF: Dataset[Row], dictDF: Dataset[Row]): Unit = {
    val original = originalDF.head().getLong(0)
    val dict = dictDF.head().getLong(0)
    // scalastyle:off println
    println(s"Random element count distinct value:$original")
    println(s"Random element generate dict num:$dict")
    assert(original == dict)
  }

  def genRandomData(spark: SparkSession, colName: String, count: Int, length: Int): Dataset[Row] = {
    var schema = new StructType()
    val column = StructField(colName, StringType)
    schema = schema.add(column)
    spark.createDataFrame(
      spark.sparkContext.parallelize(0.until(count)
        .map(_ => Row(RandomStringUtils.randomAlphabetic(length)))), schema)
  }

  def genDataWithWrapEncodeCol(colName: String, df: Dataset[Row]): Dataset[Row] = {
    val dictCol = dict_encode_v3(col(colName))
    df.select(df.schema.map(ty => col(ty.name)) ++ Seq(dictCol): _*)
  }

  def genDataWithWrapEncodeCol(spark: SparkSession, colName: String, count: Int, length: Int): Dataset[Row] = {
    val df = genRandomData(spark, colName, count, length)
    val dictCol = dict_encode_v3(col(colName))
    df.select(df.schema.map(ty => col(ty.name)) ++ Seq(dictCol): _*)
  }
}
