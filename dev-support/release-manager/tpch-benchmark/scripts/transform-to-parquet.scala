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

// export HADOOP_CONF_DIR=/opt/hadoop-3.2.1/etc/hadoop
// bin/spark-shell --executor-cores 1 --num-executors 8 --master yarn --conf spark.sql.files.maxPartitionBytes=30m

spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/lineitem").registerTempTable("lineitem")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/orders").registerTempTable("orders")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/customer").registerTempTable("customer")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/partsupp").registerTempTable("partsupp")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/part").registerTempTable("part")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/supplier").registerTempTable("supplier")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/nation").registerTempTable("nation")
spark.read.format("orc").load("/user/hive/warehouse/tpch_flat_orc_2.db/region").registerTempTable("region")

var lt = spark.sql("""
select
  l_orderkey,
  l_partkey,
  l_suppkey,
  l_linenumber,
  CAST(l_quantity AS DECIMAL(10,2)) as l_quantity,
  CAST(l_extendedprice AS DECIMAL(14,2)) as l_extendedprice,
  CAST(l_discount AS DECIMAL(4,3)) as l_discount,
  CAST(l_tax AS DECIMAL(4,3)) as l_tax,
  l_returnflag,
  l_linestatus,
  to_date(l_shipdate) as l_shipdate,
  to_date(l_commitdate) as l_commitdate,
  to_date(l_receiptdate) as l_receiptdate,
  l_shipinstruct,
  l_shipmode,
  l_comment
from lineitem
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/lineitem")

lt = spark.sql("""
select
  o_orderkey,
  o_custkey,
  o_orderstatus,
  CAST(o_totalprice AS DECIMAL(21,3)) as o_totalprice,
  to_date(o_orderdate) as o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
from orders
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/orders")


lt = spark.sql("""
select
  c_custkey,
  c_name,
  c_address,
  c_nationkey,
  c_phone,
  CAST(c_acctbal AS DECIMAL(15,3)) as c_acctbal,
  c_mktsegment,
  c_comment
from customer
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/customer")

lt = spark.sql("""
select
  ps_partkey,
  ps_suppkey,
  ps_availqty,
  CAST(ps_supplycost AS DECIMAL(15,3)) as ps_supplycost,
  ps_comment
from partsupp
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/partsupp")

lt = spark.sql("""
select
  p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  CAST(p_retailprice AS DECIMAL(15,3)) as p_retailprice,
  p_comment
from part
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/part")


lt = spark.sql("""
select
  s_suppkey,
  s_name,
  s_address,
  s_nationkey,
  s_phone,
  CAST(s_acctbal AS DECIMAL(15,3)) as s_acctbal,
  s_comment
from supplier
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/supplier")


lt = spark.sql("""
select
  *
from nation
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/nation")


lt = spark.sql("""
select
  *
from region
""").cache()

lt.write.format("parquet").save("/user/hive/warehouse/xxyu/region")