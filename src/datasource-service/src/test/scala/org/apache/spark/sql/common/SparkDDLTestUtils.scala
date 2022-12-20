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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.kylin.common.util.RandomUtil

import org.apache.spark.sql.{KylinSession, SparderEnv, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.util.Utils

object SparkDDLTestUtils {

  def prepare(): Unit = {
    val conf = new SparkConf(false)
    val warehouse = s"./spark-warehouse/${RandomUtil.randomUUIDStr()}";
    val file = new File(warehouse)
    if (file.exists()) {
      FileUtils.deleteDirectory(file)
    }
    conf.set(StaticSQLConf.WAREHOUSE_PATH.key, warehouse)
    // Copied from TestHive
    // HDFS root scratch dir requires the write all (733) permission. For each connecting user,
    // an HDFS scratch dir: ${hive.exec.scratchdir}/<username> is created, with
    // ${hive.scratch.dir.permission}. To resolve the permission issue, the simplest way is to
    // delete it. Later, it will be re-created with the right permission.
    val scratchDir = Utils.createTempDir()
    if (scratchDir.exists()) {
      FileUtils.deleteDirectory(scratchDir)
    }
    conf.set(ConfVars.SCRATCHDIR.varname, scratchDir.toString)
    conf.set("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:db;create=true")
    KylinSession.initLogicalViewConfig(conf)
    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName(getClass.getSimpleName)
      .config("fs.file.impl", classOf[DebugFilesystem].getCanonicalName)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate
    SparderEnv.setSparkSession(sparkSession)
    prepareTable
  }

  def prepareTable(): Unit = {
    val spark = SparderEnv.getSparkSession
    spark.sql("CREATE DATABASE if not exists SSB")
    spark.sql("drop table if exists `ssb`.`lineorder`")
    spark.sql(
      s"""
         |CREATE TABLE if not exists `ssb`.`lineorder`(
         |  `lo_orderkey` bigint,
         |  `lo_linenumber` bigint,
         |  `lo_custkey` int,
         |  `lo_partkey` int,
         |  `lo_suppkey` int,
         |  `lo_orderdate` int,
         |  `lo_orderpriotity` string,
         |  `lo_shippriotity` int,
         |  `lo_quantity` bigint,
         |  `lo_extendedprice` bigint,
         |  `lo_ordtotalprice` bigint,
         |  `lo_discount` bigint,
         |  `lo_revenue` bigint,
         |  `lo_supplycost` bigint,
         |  `lo_tax` bigint,
         |  `lo_commitdate` int,
         |  `lo_shipmode` string)""".stripMargin)
    spark.sql("drop view if exists `ssb`.p_lineorder")
    spark.sql(
      s"""
         |CREATE VIEW if not exists `ssb`.`p_lineorder` AS SELECT
         |  `lineorder`.`lo_orderkey`,
         |  `lineorder`.`lo_linenumber`,
         |  `lineorder`.`lo_custkey`,
         |  `lineorder`.`lo_partkey`,
         |  `lineorder`.`lo_suppkey`,
         |  `lineorder`.`lo_orderdate`,
         |  `lineorder`.`lo_orderpriotity`,
         |  `lineorder`.`lo_shippriotity`,
         |  `lineorder`.`lo_quantity`,
         |  `lineorder`.`lo_extendedprice`,
         |  `lineorder`.`lo_ordtotalprice`,
         |  `lineorder`.`lo_discount`,
         |  `lineorder`.`lo_revenue`,
         |  `lineorder`.`lo_supplycost`,
         |  `lineorder`.`lo_tax`,
         |  `lineorder`.`lo_commitdate`,
         |  `lineorder`.`lo_shipmode`,
         |  `lineorder`.`lo_extendedprice`*`lineorder`.`lo_discount` AS `V_REVENUE`
         |FROM `ssb`.`LINEORDER`
       """.stripMargin)
    spark.sql("drop table if exists `ssb`.`customer`")
    spark.sql(
      s"""
         |CREATE TABLE if not exists `ssb`.`customer`(
         |  `c_custkey` int,
         |  `c_name` string,
         |  `c_address` string,
         |  `c_city` string,
         |  `c_nation` string,
         |  `c_region` string,
         |  `c_phone` string,
         |  `c_mktsegment` string)
       """.stripMargin)
    spark.sql(
      s"""
         |CREATE TABLE if not exists `ssb`.`unload_table`(
         |  `c1` int,
         |  `c2` string)
       """.stripMargin)
    spark.sql(
      s"""
         |CREATE TABLE if not exists `ssb`.`ke_table1`(
         |  `c1` int,
         |  `c2` string)
       """.stripMargin)
  }
}
