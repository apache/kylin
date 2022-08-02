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
-- Unless required by applicable law or agreed to in writing, softwarea
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT


COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID1 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID3 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN ID4 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price1 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price2 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price3 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price5 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price6 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN price7 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name1 ELSE cast(NULL as string) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name1 ELSE cast(NULL as varchar(254)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name1 ELSE cast(NULL as char) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name2 ELSE cast(NULL as string) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name2 ELSE cast(NULL as varchar(254)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name2 ELSE cast(NULL as char) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name3 ELSE cast(NULL as string) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name3 ELSE cast(NULL as varchar(254)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name3 ELSE cast(NULL as char) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as bigint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as int) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as float) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as double) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as decimal(19,6)) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as tinyint) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN name4 ELSE cast(NULL as smallint) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN time1 ELSE cast(NULL as date) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN time1 ELSE cast(NULL as timestamp) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN time2 ELSE cast(NULL as date) END)),
COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN time2 ELSE cast(NULL as timestamp) END)),

COUNT(DISTINCT (CASE WHEN time1 = DATE'2012-10-30' THEN flag ELSE cast(NULL as boolean) END))


FROM TEST_MEASURE AS TEST_MEASURE