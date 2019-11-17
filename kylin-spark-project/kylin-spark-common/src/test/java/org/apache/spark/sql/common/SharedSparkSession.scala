/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
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
