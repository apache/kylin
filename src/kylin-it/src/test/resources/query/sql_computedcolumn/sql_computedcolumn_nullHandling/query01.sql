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


select count(ifnull(ID2,132322342)),  --test bigint null value
       count(ifnull(ID3,14123)),  --test long null value
       count(ifnull(ID4,313)),  --test int null value
       count(ifnull(price1,12.34)),  --test float null value
       count(ifnull(price2,124.44)),  --test double null value
       count(ifnull(price3,14.242343)),  --test decimal(19,6)) null value
       count(ifnull(price5,2)),  --test short null value
       count(ifnull(price6,7)),  --test tinyint null value
       count(ifnull(price7,1)),  --test smallint null value
       count(ifnull(name1,'FT')),  --test string null value
       count(ifnull(name2,'FT')),  --test varchar(254)) null value
       count(ifnull(name3,'FT')),  --test char null value
       count(ifnull(name4,2)),  --test byte null value
       count(ifnull(time1,date'2014-3-31')),  --test date null value
       count(ifnull(time2,timestamp'2019-08-08 16:33:41')),  --test timestamp null value
       count(ifnull(flag,true)), --test boolean null value
       count(isnull(ID2)),  --test bigint null value
       count(isnull(ID3)),  --test long null value
       count(isnull(ID4)),  --test int null value
       count(isnull(price1)),  --test float null value
       count(isnull(price2)),  --test double null value
       count(isnull(price3)),  --test decimal(19,6)) null value
       count(isnull(price5)),  --test short null value
       count(isnull(price6)),  --test tinyint null value
       count(isnull(price7)),  --test smallint null value
       count(isnull(name1)),  --test string null value
       count(isnull(name2)),  --test varchar(254)) null value
       count(isnull(name3)),  --test char null value
       count(isnull(name4)),  --test byte null value
       count(isnull(time1)),  --test date null value
       count(isnull(time2)),  --test timestamp null value
       count(isnull(flag)) --test boolean null value
from TEST_MEASURE

