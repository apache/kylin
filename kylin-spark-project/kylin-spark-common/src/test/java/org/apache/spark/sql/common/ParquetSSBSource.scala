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

import org.scalatest.Suite

trait ParquetSSBSource extends SharedSparkSession {
  self: Suite =>

  lazy val SSB_BASE_DIR = "../../spark-project/data/ssb"
  val FORMAT =
    "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat"
  val delimiter = "|"

  def ssbDataFolder(tableName: String): String = s"$SSB_BASE_DIR/$tableName/"

  override def beforeAll() {
    super.beforeAll()

    sql("drop table if exists `customer`")
    sql(
      s"""
         |CREATE TABLE if not exists `customer`(
         |  `c_custkey` int,
         |  `c_name` string,
         |  `c_address` string,
         |  `c_city` string,
         |  `c_nation` string,
         |  `c_region` string,
         |  `c_phone` string,
         |  `c_mktsegment` string)
         |  USING $FORMAT
         |  OPTIONS (path "${ssbDataFolder("customer")}",
         |  header "false", delimiter "$delimiter")
       """.stripMargin)

    sql("drop table if exists `dates`")
    sql(
      s"""
         |CREATE TABLE if not exists `dates`(
         |  `d_datekey` int,
         |  `d_date` string,
         |  `d_dayofweek` string,
         |  `d_month` string,
         |  `d_year` int,
         |  `d_yearmonthnum` int,
         |  `d_yearmonth` string,
         |  `d_daynuminweek` int,
         |  `d_daynuminmonth` int,
         |  `d_daynuminyear` int,
         |  `d_monthnuminyear` int,
         |  `d_weeknuminyear` int,
         |  `d_sellingseason` string,
         |  `d_lastdayinweekfl` int,
         |  `d_lastdayinmonthfl` int,
         |  `d_holidayfl` int,
         |  `d_weekdayfl` int)
         |  USING $FORMAT
         |  OPTIONS (path "${ssbDataFolder("dates")}",
         |  header "false", delimiter "$delimiter")
       """.stripMargin)

    sql("drop table if exists `part`")
    sql(
      s"""
         |CREATE TABLE if not exists `part`(
         |  `p_partkey` int,
         |  `p_name` string,
         |  `p_mfgr` string,
         |  `p_category` string,
         |  `p_brand` string,
         |  `p_color` string,
         |  `p_type` string,
         |  `p_size` int,
         |  `p_container` string)
         |  USING $FORMAT
         |  OPTIONS (path "${ssbDataFolder("part")}",
         |  header "false", delimiter "$delimiter")
       """.stripMargin)

    sql("drop table if exists `supplier`")
    sql(
      s"""
         |CREATE TABLE if not exists `supplier`(
         |  `s_suppkey` int,
         |  `s_name` string,
         |  `s_address` string,
         |  `s_city` string,
         |  `s_nation` string,
         |  `s_region` string,
         |  `s_phone` string)
         |  USING $FORMAT
         |  OPTIONS (path "${ssbDataFolder("supplier")}",
         |  header "false", delimiter "$delimiter")
       """.stripMargin)

    sql("drop table if exists `lineorder`")
    sql(
      s"""
         |CREATE TABLE if not exists `lineorder`(
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
         |  `lo_shipmode` string)
         |  USING $FORMAT
         |  OPTIONS (path "${ssbDataFolder("lineorder")}",
         |  header "false", delimiter "$delimiter")
       """.stripMargin)

    sql("drop view if exists p_lineorder")
    sql(
      s"""
         |CREATE VIEW if not exists `p_lineorder` AS SELECT
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
         |FROM `LINEORDER`
       """.stripMargin)
  }

}
