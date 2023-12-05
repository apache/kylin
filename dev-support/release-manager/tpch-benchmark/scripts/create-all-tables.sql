--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

-------------------------------------------------------------------------
-------------------------------------------------------------------------
--------------------------- Define & Formula ----------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

-- 零售总价/l_extendedprice = L_QUANTITY * P_RETAILPRICE
-- 销售收入/revenue =  l_extendedprice * (1 - l_discount)
-- 实际利润/profit  = revenue - (ps_supplycost * l_quantity)
-- 实际收费/charge  = l_extendedprice*(1-l_discount)*(1+l_tax)
-- L_LINESTATUS      : "O" if L_SHIPDATE > CURRENTDATE "F" otherwise


-------------------------------------------------------------------------
-------------------------------------------------------------------------
------------------------ Appendable Fact Table --------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS TPCH_CN;
USE TPCH_CN;

DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS partsupp;
DROP TABLE IF EXISTS part;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;


CREATE EXTERNAL TABLE lineitem(
    l_orderkey bigint ,
    l_partkey bigint ,
    l_suppkey bigint ,
    l_linenumber int ,
    l_quantity DECIMAL(10,2),
    l_extendedprice DECIMAL(14,2),
    l_discount DECIMAL(4,3) ,
    l_tax DECIMAL(4,3),
    l_returnflag string,
    l_linestatus string,
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct string ,
    l_shipmode string ,
    l_comment string
)
COMMENT 'Single-Row-of-Order-6M'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/lineitem')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/lineitem'
;


CREATE EXTERNAL TABLE orders(
    o_orderkey bigint,
    o_custkey bigint ,
    o_orderstatus string ,
    o_totalprice DECIMAL(21,3) ,
    o_orderdate date ,
    o_orderpriority string ,
    o_clerk string ,
    o_shippriority int ,
    o_comment string
)
COMMENT 'Order-1_5M'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/orders')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/orders'
;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
---------------------------- Small Fact Table ---------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

CREATE EXTERNAL TABLE customer(
    c_custkey bigint,
    c_name string,
    c_address string,
    c_nationkey bigint,
    c_phone string ,
    c_acctbal decimal(15,3),
    c_mktsegment string,
    c_comment string
)
COMMENT 'Customers-150k'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/customer')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/customer'
;



CREATE EXTERNAL TABLE partsupp(
    ps_partkey bigint,
    ps_suppkey bigint,
    ps_availqty int,
    ps_supplycost DECIMAL(15,3),
    ps_comment string
)
COMMENT 'Part-Supplier-Relation-800k'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/partsupp')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/partsupp'
;


CREATE EXTERNAL TABLE part(
    p_partkey bigint,
    p_name string,
    p_mfgr string,
    p_brand string,
    p_type string,
    p_size int ,
    p_container string,
    p_retailprice DECIMAL(15,3),
    p_comment string
)
COMMENT 'Part-200k'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/part')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    'hdfs://kylin5-machine:9000/user/data/tpch/part'
;


CREATE EXTERNAL TABLE supplier(
   s_suppkey bigint,
   s_name string,
   s_address string,
   s_nationkey bigint,
   s_phone string,
   s_acctbal DECIMAL(15,3),
   s_comment string)
COMMENT 'Supplier-10k'
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES (
    'path'='hdfs://kylin5-machine:9000/user/data/tpch/supplier')
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    'hdfs://kylin5-machine:9000/user/data/tpch/supplier'
;


-------------------------------------------------------------------------
-------------------------------------------------------------------------
---------------------------- Dimension Table ----------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

CREATE EXTERNAL TABLE nation(
    n_nationkey bigint,
    n_name string,
    n_regionkey bigint,
    n_comment string
)
COMMENT 'Nation-25'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/nation')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/nation'
;

CREATE EXTERNAL TABLE region(
    r_regionkey bigint,
    r_name string ,
    r_comment string
)
COMMENT 'Region-5'
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    WITH SERDEPROPERTIES ('path'='hdfs://kylin5-machine:9000/user/data/tpch/region')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 'hdfs://kylin5-machine:9000/user/data/tpch/region'
;