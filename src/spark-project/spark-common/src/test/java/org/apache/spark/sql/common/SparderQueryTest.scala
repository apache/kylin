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
package org.apache.spark.sql.common

import org.apache.kylin.common.{KapConfig, QueryContext}

import java.sql.Types
import java.util.{List, TimeZone}
import org.apache.kylin.metadata.query.StructField
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SparderTypeUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util

object SparderQueryTest extends Logging {

  def same(sparkDF: DataFrame,
                     kylinAnswer: DataFrame,
                     checkOrder: Boolean = false): Boolean = {
    checkAnswerBySeq(castDataType(sparkDF, kylinAnswer), kylinAnswer.collect(), checkOrder) match {
      case Some(errorMessage) =>
        logInfo(errorMessage)
        false
      case None => true
    }
  }

  def checkAnswer(sparkDF: DataFrame,
                  kylinAnswer: DataFrame,
                  checkOrder: Boolean = false): String = {
    checkAnswerBySeq(castDataType(sparkDF, kylinAnswer), kylinAnswer.collect(), checkOrder) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }

  /**
    * Runs the plan and makes sure the answer matches the expected result.
    * If there was exception during the execution or the contents of the DataFrame does not
    * match the expected result, an error message will be returned. Otherwise, a [[None]] will
    * be returned.
    *
    * @param sparkDF     the [[org.apache.spark.sql.DataFrame]] to be executed
    * @param kylinAnswer the expected result in a [[Seq]] of [[org.apache.spark.sql.Row]]s.
    * @param checkToRDD  whether to verify deserialization to an RDD. This runs the query twice.
    */
  def checkAnswerBySeq(sparkDF: DataFrame,
                       kylinAnswer: Seq[Row],
                       checkOrder: Boolean = false,
                       checkToRDD: Boolean = true): Option[String] = {
    if (checkToRDD) {
      sparkDF.rdd.count() // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer = try sparkDF.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${sparkDF.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(sparkAnswer, kylinAnswer, checkOrder).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
        |${sparkDF.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }

  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // compare string .
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP)
      case db: Double if db.isNaN || db.isInfinite => None
      case db: Double => BigDecimal.apply(db).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString.toDouble
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def sameRows(sparkAnswer: Seq[Row],
               kylinAnswer: Seq[Row],
               isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(sparkAnswer, isSorted) != prepareAnswer(kylinAnswer,
      isSorted)) {
      val errorMessage =
        s"""
           |== Results ==
           |${
          sideBySide(
            s"== Kylin Answer - ${kylinAnswer.size} ==" +:
              prepareAnswer(kylinAnswer, isSorted).map(_.toString()),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
              prepareAnswer(sparkAnswer, isSorted).map(_.toString())
          ).mkString("\n")
        }
        """.stripMargin
      return Some(errorMessage)
    }
    None
  }


  /**
    * Runs the plan and makes sure the answer is within absTol of the expected result.
    *
    * @param actualAnswer   the actual result in a [[Row]].
    * @param expectedAnswer the expected result in a[[Row]].
    * @param absTol         the absolute tolerance between actual and expected answers.
    */
  protected def checkAggregatesWithTol(actualAnswer: Row,
                                       expectedAnswer: Row,
                                       absTol: Double) = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(
          math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAsyncResultData(dataFrame: DataFrame,
                           sparkSession: SparkSession): Unit = {
    val path = KapConfig.getInstanceFromEnv.getAsyncResultBaseDir(null) + "/" +
      QueryContext.current.getQueryId
    val rows = sparkSession.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .load(path)
    val maybeString = checkAnswer(dataFrame, rows)

  }

  private def isCharType(dataType: Integer): Boolean = {
    if (dataType == Types.CHAR || dataType == Types.VARCHAR) {
      return true
    }
    false
  }

  private def isIntType(structField: StructField): Boolean = {
    val intList = scala.collection.immutable.List(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT)
    val dataType = structField.getDataType();
    if (intList.contains(dataType)) {
      return true
    }
    false
  }

  private def isBinaryType(structField: StructField): Boolean = {
    val intList = scala.collection.immutable.List(Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY)
    val dataType = structField.getDataType();
    if (intList.contains(dataType)) {
      return true
    }
    false
  }

  def isSameDataType(cubeStructField: StructField, sparkStructField: StructField): Boolean = {
    if (cubeStructField.getDataType() == sparkStructField.getDataType()) {
      return true
    }
    // calcite dataTypeName = "ANY"
    if (cubeStructField.getDataType() == 2000) {
      return true
    }
    if (isCharType(cubeStructField.getDataType()) && isCharType(sparkStructField.getDataType())) {
      return true
    }
    if (isIntType(cubeStructField) && isIntType(sparkStructField)) {
      return true
    }
    if (isBinaryType(cubeStructField) && isBinaryType(sparkStructField)) {
      return true
    }
    false
  }

  def castDataType(sparkResult: DataFrame, cubeResult: DataFrame): DataFrame = {
    val newNames = sparkResult.schema.names
      .zipWithIndex
      .map(name => name._1.replaceAll("\\.", "_") + "_" + name._2).toSeq
    val newDf = sparkResult.toDF(newNames: _*)
    val columns = newDf.schema.zip(cubeResult.schema).map {
      case (sparkField, kylinField) =>
        if (!sparkField.dataType.sameType(kylinField.dataType)) {
          col(sparkField.name).cast(kylinField.dataType)
        } else {
          col(sparkField.name)
        }
    }
    newDf.select(columns: _*)
  }
}
