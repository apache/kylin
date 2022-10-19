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


select ifnull(ID2,132322342),  --test bigint null value
       ifnull(ID3,14123),  --test long null value
       ifnull(ID4,313),  --test int null value
       ifnull(price1,12.34),  --test float null value
       ifnull(price2,124.44),  --test double null value
       ifnull(price3,14.242343),  --test decimal(19,6) null value
       ifnull(price5,2),  --test short null value
       ifnull(price6,7),  --test tinyint null value
       ifnull(price7,1),  --test smallint null value
       ifnull(name1,'FT'),  --test string null value
       ifnull(name2,'FT'),  --test varchar(254) null value
       ifnull(name3,'FT'),  --test char null value
       ifnull(name4,2),  --test byte null value
       ifnull(time1,date'2014-3-31'),  --test date null value
       ifnull(time2,timestamp'2019-08-08 16:33:41.061'),  --test timestamp null value
       ifnull(flag,true), --test boolean null value
       nvl(ID2,132322342),  --test bigint null value
       nvl(ID3,14123),  --test long null value
       nvl(ID4,313),  --test int null value
       nvl(price1,12.34),  --test float null value
       nvl(price2,124.44),  --test double null value
       nvl(price3,14.242343),  --test decimal(19,6) null value
       nvl(price5,2),  --test short null value
       nvl(price6,7),  --test tinyint null value
       nvl(price7,1),  --test smallint null value
       nvl(name1,'FT'),  --test string null value
       nvl(name2,'FT'),  --test varchar(254) null value
       nvl(name3,'FT'),  --test char null value
       nvl(name4,2),  --test byte null value
       nvl(time1,date'2014-3-31'),  --test date null value
       nvl(time2,timestamp'2019-08-08 16:33:41.061'),  --test timestamp null value
       nvl(flag,true),
       isnull(ID2),  --test bigint null value
       isnull(ID3),  --test long null value
       isnull(ID4),  --test int null value
       isnull(price1),  --test float null value
       isnull(price2),  --test double null value
       isnull(price3),  --test decimal(19,6) null value
       isnull(price5),  --test short null value
       isnull(price6),  --test tinyint null value
       isnull(price7),  --test smallint null value
       isnull(name1),  --test string null value
       isnull(name2),  --test varchar(254) null value
       isnull(name3),  --test char null value
       isnull(name4),  --test byte null value
       isnull(time1),  --test date null value
       isnull(time2),  --test timestamp null value
       isnull(flag) --test boolean null value
from TEST_MEASURE

