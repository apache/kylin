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

import org.scalatest.Suite

trait TPCHSource extends SharedSparkSession {
  self: Suite =>

  val TPCH_BASE_DIR = "../spark-project/data"
  val FORMAT = "com.databricks.spark.csv"
  def tpchDataFolder(tableName: String): String = s"$TPCH_BASE_DIR/$tableName/"

  override def beforeAll() {
    super.beforeAll()
    sql(
      s"""CREATE TABLE if not exists lineitem(l_orderkey integer,
        l_partkey integer, l_suppkey integer,
      l_linenumber integer,
      l_quantity double, l_extendedprice double, l_discount double, l_tax double,
      l_returnflag string,
      l_linestatus string, l_shipdate string, l_commitdate string, l_receiptdate string,
      l_shipinstruct string,
      l_shipmode string, l_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("lineitem")}",
      header "false", delimiter "|")""".stripMargin)

    sql(
      s"""CREATE TABLE if not exists orders_csv(
         |o_orderkey integer, o_custkey integer,
         |    o_orderstatus VARCHAR(1),
         |    o_totalprice double,
         |    o_orderdate string,
         |    o_orderpriority VARCHAR(15),
         |    o_clerk VARCHAR(15),
         |    o_shippriority integer,
         |    o_comment VARCHAR(79)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("orders")}",
      header "false", delimiter "|")""".stripMargin)

    sql(
      s"""create table if not exists orders
         |USING org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat as
         |select * from orders_csv""".stripMargin)

    sql(
      s"""CREATE TABLE if not exists partsupp(
         | ps_partkey integer, ps_suppkey integer,
         |    ps_availqty integer, ps_supplycost double,
         |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists supplier(
             s_suppkey integer, s_name string, s_address string,
             s_nationkey integer,
         |      s_phone string, s_acctbal double, s_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("supplier")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists part(p_partkey integer, p_name string,
         |      p_mfgr string, p_brand string, p_type string, p_size integer, p_container string,
         |      p_retailprice double,
         |      p_comment string)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("part")}",
      header "false", delimiter "|")""".stripMargin)

    sql(
      s"""CREATE TABLE if not exists customer(
         | c_custkey INTEGER,
         |    c_name VARCHAR(25),
         |    c_address VARCHAR(40),
         |    c_nationkey INTEGER,
         |    c_phone VARCHAR(15),
         |    c_acctbal double,
         |    c_mktsegment VARCHAR(10),
         |    c_comment VARCHAR(117)
         |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("customer")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists custnation(
         | cn_nationkey integer, cn_name VARCHAR(25),
         |    cn_regionkey integer, cn_comment VARCHAR(152)
         |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists custregion(
         | cr_regionkey integer, cr_name VARCHAR(25),
         |    cr_comment VARCHAR(152)
         |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists suppnation(
         | sn_nationkey integer, sn_name VARCHAR(25),
         |    sn_regionkey integer, sn_comment VARCHAR(152)
         |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("nation")}",
      header "false", delimiter "|")""".stripMargin)


    sql(
      s"""CREATE TABLE if not exists suppregion(
         | sr_regionkey integer, sr_name VARCHAR(25),
         |    sr_comment VARCHAR(152)
         |)
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("region")}",
      header "false", delimiter "|")""".stripMargin)

    sql(
      s"""create view if not exists v_lineitem as
         |select
         |    lineitem.*,
         |
         |    year(l_shipdate) as l_shipyear,
         |    case when l_commitdate < l_receiptdate then 1 else 0 end as l_receiptdelayed,
         |    case when l_shipdate < l_commitdate then 0 else 1 end as l_shipdelayed,
         |
         |    l_extendedprice * (1 - l_discount) as l_saleprice,
         |    l_extendedprice * (1 - l_discount) * l_tax as l_taxprice,
         |    ps_supplycost * l_quantity as l_supplycost
         |from
         |    lineitem
         |    inner join partsupp on l_partkey=ps_partkey and l_suppkey=ps_suppkey""".stripMargin)

    sql(
      s"""create view if not exists v_orders as
         |select
         |    orders.*,
         |    year(o_orderdate) as o_orderyear
         |from
         |    orders
         |""".stripMargin)

    sql(
      s"""
         |create view if not exists v_partsupp as
         |select
         |    partsupp.*,
         |    ps_supplycost * ps_availqty as ps_partvalue
         |from
         |    partsupp""".stripMargin)


    //    TestHive.setConf(DruidPlanner.SPARKLINEDATA_CACHE_TABLES_TOCHECK.key,
    //      "orderLineItemPartSupplierBase,suppregion,suppnation," +
    //        "custregion,custnation,customer,part,supplier,partsupp,orders,lineitembase")

    /*
     * for -ve testing only
     */
    sql(
      s"""CREATE TABLE if not exists partsupp2(
         | ps_partkey integer, ps_suppkey integer,
         |    ps_availqty integer, ps_supplycost double,
         |    ps_comment VARCHAR(199)
    )
      USING com.databricks.spark.csv
      OPTIONS (path "${tpchDataFolder("partsupp")}",
      header "false", delimiter "|")""".stripMargin)

  }


}
