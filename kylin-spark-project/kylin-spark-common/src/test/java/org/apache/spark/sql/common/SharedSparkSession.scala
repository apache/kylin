/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.common

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SharedSparkSession
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {
  self: Suite =>
  @transient private var _sc: SparkContext = _
  @transient private var _spark: SparkSession = _
  @transient private var _jsc: JavaSparkContext = _
  var _conf: SparkConf = new SparkConf()
  val master: String = "local[4]"

  def sc: SparkContext = _sc

  protected implicit def spark: SparkSession = _spark

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()
    val file = new File("./spark-warehouse")
    if (file.exists()) {
      FileUtils.deleteDirectory(file)
    }
    initSpark()
  }

   def initSpark(): Unit = {
    _spark = SparkSession.builder
      //      .enableHiveSupport()
      .master(master)
      .appName(getClass.getSimpleName)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.columnVector.offheap.enabled", "true")
      .config("spark.memory.fraction", "0.1")
      .config("fs.file.impl", classOf[DebugFilesystem].getCanonicalName)
      //      .config("spark.sql.adaptive.enabled", "true")
      .config(conf)
      .getOrCreate
    _jsc = new JavaSparkContext(_spark.sparkContext)
    _sc = _spark.sparkContext
  }

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  override def afterAll() {
    try {
      _spark.stop()
      _sc = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    DebugFilesystem.assertNoOpenStreams()
  }

  def sql(sql: String): DataFrame = {
    logInfo(s"Executor sql: $sql")
    spark.sql(sql)
  }
  /**
    * Drops global temporary view `viewNames` after calling `f`.
    */
  protected def withGlobalTempView(viewNames: String*)(f: => Unit): Unit = {
    try f finally {
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // global temp views that never got created.
      try viewNames.foreach(spark.catalog.dropGlobalTempView) catch {
        case _: NoSuchTableException =>
      }
    }
  }

  /**
    * Drops table `tableName` after calling `f`.
    */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
    * Drops view `viewName` after calling `f`.
    */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    try f finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }
}
