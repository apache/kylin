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

-- How to execute this file.
-- mysql -u root -p123456 hive < this-file
-- mysql -u root -p123456 hive -e "update COLUMNS_V2 set COMMENT = '该条目所属的订单编号,外键,联合主键' where COLUMN_NAME = 'l_orderkey' ;"

alter table COLUMNS_V2 modify column COMMENT varchar(256) character set utf8mb4;

-- lineitem
update COLUMNS_V2 set COMMENT = '该条目所属的订单编号,外键,联合主键' where COLUMN_NAME = 'l_orderkey' ;
update COLUMNS_V2 set COMMENT = '销售零件编号' where COLUMN_NAME = 'l_partkey' ;
update COLUMNS_V2 set COMMENT = '销售零件所属供应商编号' where COLUMN_NAME = 'l_suppkey' ;
update COLUMNS_V2 set COMMENT = '该条目在所属订单的序号:1-7,联合主键' where COLUMN_NAME = 'l_linenumber' ;
update COLUMNS_V2 set COMMENT = '销售数量' where COLUMN_NAME = 'l_quantity' ;
update COLUMNS_V2 set COMMENT = '零售价格:L_QUANTITY * P_RETAILPRICE' where COLUMN_NAME = 'l_extendedprice' ;
update COLUMNS_V2 set COMMENT = '促销折扣，百分比' where COLUMN_NAME = 'l_discount' ;
update COLUMNS_V2 set COMMENT = '税率，税由顾客支付，百分比' where COLUMN_NAME = 'l_tax' ;
update COLUMNS_V2 set COMMENT = '退货标志位:R,A,F' where COLUMN_NAME = 'l_returnflag' ;
update COLUMNS_V2 set COMMENT = '条目发送状态:O,F' where COLUMN_NAME = 'l_linestatus' ;
update COLUMNS_V2 set COMMENT = '配送日期' where COLUMN_NAME = 'l_shipdate' ;
update COLUMNS_V2 set COMMENT = '承诺送达日期' where COLUMN_NAME = 'l_commitdate' ;
update COLUMNS_V2 set COMMENT = '实际送达日期' where COLUMN_NAME = 'l_receiptdate' ;
update COLUMNS_V2 set COMMENT = '配送方式:DELIVER IN PERSON,COLLECT COD,NONE,TAKE BACK RETURN' where COLUMN_NAME = 'l_shipinstruct' ;
update COLUMNS_V2 set COMMENT = '配送交通方式:REG AIR,AIR,RAIL,SHIP,TRUCK,MAIL' where COLUMN_NAME = 'l_shipmode' ;
update COLUMNS_V2 set COMMENT = '订单备注' where COLUMN_NAME = 'l_comment';


-- orders
update COLUMNS_V2 set COMMENT = '订单编号，主键' where COLUMN_NAME = 'o_orderkey' ;
update COLUMNS_V2 set COMMENT = '顾客编号，外键' where COLUMN_NAME = 'o_custkey' ;
update COLUMNS_V2 set COMMENT = '订单状态，取决于l_linestatus，可选值:O,F,P' where COLUMN_NAME = 'o_orderstatus' ;
update COLUMNS_V2 set COMMENT = '条目零售价格总合:sum(L_EXTENDEDPRICE*(1+L_TAX)*(1-L_DISCOUNT))' where COLUMN_NAME = 'o_totalprice' ;
update COLUMNS_V2 set COMMENT = '订单日期' where COLUMN_NAME = 'o_orderdate' ;
update COLUMNS_V2 set COMMENT = '订单优先级' where COLUMN_NAME = 'o_orderpriority' ;
update COLUMNS_V2 set COMMENT = '职员编号:Clerk#000000000' where COLUMN_NAME = 'o_clerk' ;
update COLUMNS_V2 set COMMENT = '配送优先级' where COLUMN_NAME = 'o_shippriority' ;
update COLUMNS_V2 set COMMENT = '订单备注'  where COLUMN_NAME = 'o_comment';

-- customer
update COLUMNS_V2 set COMMENT = '顾客编号，主键' where COLUMN_NAME = 'c_custkey' ;
update COLUMNS_V2 set COMMENT = '顾客称呼' where COLUMN_NAME = 'c_name' ;
update COLUMNS_V2 set COMMENT = '顾客地址' where COLUMN_NAME = 'c_address' ;
update COLUMNS_V2 set COMMENT = '顾客国家，外键' where COLUMN_NAME = 'c_nationkey' ;
update COLUMNS_V2 set COMMENT = '顾客手机号码' where COLUMN_NAME = 'c_phone' ;
update COLUMNS_V2 set COMMENT = '顾客余额，范围:[-999.99, 9999.99]' where COLUMN_NAME = 'c_acctbal' ;
update COLUMNS_V2 set COMMENT = '可选值:AUTOMOBILE,BUILDING,FURNITURE,MACHINERY,HOUSEHOLD' where COLUMN_NAME = 'c_mktsegment' ;
update COLUMNS_V2 set COMMENT = '_'  where COLUMN_NAME = 'c_comment';


-- PS
update COLUMNS_V2 set COMMENT = '零件编号,外键,联合主键' where COLUMN_NAME = 'ps_partkey' ;
update COLUMNS_V2 set COMMENT = '供应商编号,外键,联合主键' where COLUMN_NAME = 'ps_suppkey' ;
update COLUMNS_V2 set COMMENT = '该供应商持有该零件的剩余数量:[1,9999]' where COLUMN_NAME = 'ps_availqty' ;
update COLUMNS_V2 set COMMENT = '进货成本/批发价:[1.00,1000.00]' where COLUMN_NAME = 'ps_supplycost' ;
update COLUMNS_V2 set COMMENT = '_'  where COLUMN_NAME = 'ps_comment';


-- part
update COLUMNS_V2 set COMMENT = '零件编号,主键' where COLUMN_NAME = 'p_partkey' ;
update COLUMNS_V2 set COMMENT = '零件名称' where COLUMN_NAME = 'p_name' ;
update COLUMNS_V2 set COMMENT = '制造商:Manufacturer#[1-5]' where COLUMN_NAME = 'p_mfgr' ;
update COLUMNS_V2 set COMMENT = '品牌:Brand#[1-5][1-5]' where COLUMN_NAME = 'p_brand' ;
update COLUMNS_V2 set COMMENT = '零件类型，三个单词的组合' where COLUMN_NAME = 'p_type' ;
update COLUMNS_V2 set COMMENT = '零件尺寸:[1,50]' where COLUMN_NAME = 'p_size' ;
update COLUMNS_V2 set COMMENT = '包装方式，两个单词的组合' where COLUMN_NAME = 'p_container' ;
update COLUMNS_V2 set COMMENT = '零件零售价' where COLUMN_NAME = 'p_retailprice' ;

-- suppiler
update COLUMNS_V2 set COMMENT = '供应商编号,主键' where COLUMN_NAME = 's_suppkey' ;
update COLUMNS_V2 set COMMENT = '供应商名称' where COLUMN_NAME = 's_name' ;
update COLUMNS_V2 set COMMENT = '供应商地址' where COLUMN_NAME = 's_address' ;
update COLUMNS_V2 set COMMENT = '外键' where COLUMN_NAME = 's_nationkey' ;
update COLUMNS_V2 set COMMENT = '供应商手机号' where COLUMN_NAME = 's_phone' ;
update COLUMNS_V2 set COMMENT = '供应商账户余额' where COLUMN_NAME = 's_acctbal' ;

-- nation
update COLUMNS_V2 set COMMENT = '主键,0-24' where COLUMN_NAME = 'n_nationkey' ;
update COLUMNS_V2 set COMMENT = '25个国家:ALGERIA,ARGENTINA,BRAZIL,CANADA,EGYPT,etc' where COLUMN_NAME = 'n_name' ;

-- region
update COLUMNS_V2 set COMMENT = '外键' where COLUMN_NAME = 'n_regionkey' ;
update COLUMNS_V2 set COMMENT = '主键,0-4' where COLUMN_NAME = 'r_regionkey' ;
