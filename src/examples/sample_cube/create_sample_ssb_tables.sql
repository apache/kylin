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

USE SSB;

drop table IF EXISTS CUSTOMER;

create EXTERNAL TABLE CUSTOMER (
C_CUSTKEY     INT,
C_NAME        string,
C_ADDRESS     string,
C_CITY        string,
C_NATION      string,
C_REGION      string,
C_PHONE       string,
C_MKTSEGMENT   string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

drop table IF EXISTS DATES;

create EXTERNAL TABLE DATES (
D_DATEKEY          DATE,
D_DATE             string,
D_DAYOFWEEK        string,
D_MONTH            string,
D_YEAR             INT,
D_YEARMONTHNUM     INT,
D_YEARMONTH        string,
D_DAYNUMINWEEK     INT,
D_DAYNUMINMONTH    INT,
D_DAYNUMINYEAR     INT,
D_MONTHNUMINYEAR   INT,
D_WEEKNUMINYEAR    INT,
D_SELLINGSEASON    string,
D_LASTDAYINWEEKFL  INT,
D_LASTDAYINMONTHFL INT,
D_HOLIDAYFL        INT,
D_WEEKDAYFL        INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

drop table IF EXISTS PART;

create EXTERNAL TABLE PART (
P_PARTKEY     INT,
P_NAME        string,
P_MFGR        string,
P_CATEGORY    string,
P_BRAND       string,
P_COLOR       string,
P_TYPE        string,
P_SIZE        INT,
P_CONTAINER   string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

drop table IF EXISTS SUPPLIER;

create EXTERNAL TABLE SUPPLIER (
S_SUPPKEY     INT,
S_NAME        string,
S_ADDRESS     string,
S_CITY        string,
S_NATION      string,
S_REGION      string,
S_PHONE       string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

drop table IF EXISTS LINEORDER;

create EXTERNAL TABLE LINEORDER (
LO_ORDERKEY       BIGINT,
LO_LINENUMBER     BIGINT,
LO_CUSTKEY        INT,
LO_PARTKEY        INT,
LO_SUPPKEY        INT,
LO_ORDERDATE      DATE,
LO_ORDERPRIOTITY  string,
LO_SHIPPRIOTITY   INT,
LO_QUANTITY       BIGINT,
LO_EXTENDEDPRICE  BIGINT,
LO_ORDTOTALPRICE  BIGINT,
LO_DISCOUNT       BIGINT,
LO_REVENUE        BIGINT,
LO_SUPPLYCOST     BIGINT,
LO_TAX            BIGINT,
LO_COMMITDATE     DATE,
LO_SHIPMODE       string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

drop view IF EXISTS P_LINEORDER;

create view P_LINEORDER as
select LO_ORDERKEY,
LO_LINENUMBER,
LO_CUSTKEY,
LO_PARTKEY,
LO_SUPPKEY,
LO_ORDERDATE,
LO_ORDERPRIOTITY,
LO_SHIPPRIOTITY,
LO_QUANTITY,
LO_EXTENDEDPRICE,
LO_ORDTOTALPRICE,
LO_DISCOUNT,
LO_REVENUE,
LO_SUPPLYCOST,
LO_TAX,
LO_COMMITDATE,
LO_SHIPMODE,
LO_EXTENDEDPRICE*LO_DISCOUNT as V_REVENUE
from LINEORDER;

LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/SSB.CUSTOMER.csv' OVERWRITE INTO TABLE CUSTOMER;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/SSB.DATES.csv' OVERWRITE INTO TABLE DATES;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/SSB.LINEORDER.csv' OVERWRITE INTO TABLE LINEORDER;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/SSB.PART.csv' OVERWRITE INTO TABLE PART;
LOAD DATA INPATH '${hdfs_tmp_dir}/sample_cube/data/SSB.SUPPLIER.csv' OVERWRITE INTO TABLE SUPPLIER;