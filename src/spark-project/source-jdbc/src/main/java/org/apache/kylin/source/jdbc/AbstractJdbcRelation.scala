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

package org.apache.kylin.source.jdbc

import java.sql.{Connection, SQLException}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getSchema
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.collection.mutable

abstract class AbstractJdbcRelation(jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession) extends BaseRelation
  with PrunedFilteredScan with Logging {

  val KEY2OUTPUTS = new mutable.HashMap[CacheKey, WriteOutput]

  override lazy val schema: StructType = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val connection: Connection = dialect.createConnectionFactory(jdbcOptions)(-1)
    val schemaOption: Option[StructType] = getSchemaOption(connection, jdbcOptions)
    schemaOption.get
  }

  def getSchemaOption(conn: Connection, options: JDBCOptions): Option[StructType] = {
    val dialect = JdbcDialects.get(options.url)
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(options.tableOrQuery))
      try {
        statement.setQueryTimeout(options.queryTimeout)
        Some(getSchema(statement.executeQuery(), dialect))
      } catch {
        case e: SQLException =>
          logError("Get schema sql throws error ", e)
          None
      } finally {
        statement.close()
      }
    } catch {
      case e: SQLException =>
        logError("Get schema sql throws error ", e)
        None
    }
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext


  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val cacheKey: CacheKey = CacheKey(requiredColumns, filters, jdbcOptions.tableOrQuery)
    var writeOutput: WriteOutput = null
    if (KEY2OUTPUTS.get(cacheKey).isDefined) {
      writeOutput = KEY2OUTPUTS(cacheKey)
      log.info("Using existing writeOutput {}", writeOutput)
    } else {
      writeOutput = writeToExternal(requiredColumns, filters)
    }
    KEY2OUTPUTS.put(CacheKey(requiredColumns, filters, jdbcOptions.tableOrQuery), writeOutput)
    val rdd: RDD[Row] = buildRDD(writeOutput)
    cleanUp()
    rdd
  }

  def writeToExternal(requiredColumns: Array[String], filters: Array[Filter]): WriteOutput

  def buildRDD(writeOutput: WriteOutput): RDD[Row]

  def cleanUp(): Unit = {
    // just implement it
  }

  case class CacheKey(requiredColumns: Array[String], filters: Array[Filter], sql: String)

  trait WriteOutput

}

