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


select *
from TEST_MEASURE
where ifnull(ID2,132322342)  =  132322342  --test bigint null value
and ifnull(ID3,14123)  =  14123  --test long null value
and ifnull(ID4,313)  =  313  --test int null value
and ifnull(price1,12.34)  =  12.34  --test float null value
and ifnull(price2,124.44)  =  124.44  --test double null value
and ifnull(price3,14.242343)  =  14.242343  --test decimal(19,6) null value
and ifnull(price5,2)  =  2  --test short null value
and ifnull(price6,7)  =  7  --test tinyint null value
and ifnull(price7,1)  =  1  --test smallint null value
and ifnull(name1,'FT')  =  'FT'  --test string null value
and ifnull(name2,'FT')  =  'FT'  --test varchar(254) null value
and ifnull(name3,'FT')  =  'FT'  --test char null value
and ifnull(name4,2)  =  2  --test byte null value
and ifnull(time1,date'2014-3-31')  =  date'2014-3-31'  --test date null value
and ifnull(time2,timestamp'2019-08-08 16:33:41')  =  timestamp'2019-08-08 16:33:41'  --test timestamp null value
and ifnull(flag,true)  =  true  --test boolean null value
and nvl(ID2,132322342)  =  132322342  --test bigint null value
and nvl(ID3,14123)  =  14123  --test long null value
and nvl(ID4,313)  =  313  --test int null value
and nvl(price1,12.34)  =  12.34  --test float null value
and nvl(price2,124.44)  =  124.44  --test double null value
and nvl(price3,14.242343)  =  14.242343  --test decimal(19,6) null value
and nvl(price5,2)  =  2  --test short null value
and nvl(price6,7)  =  7  --test tinyint null value
and nvl(price7,1)  =  1  --test smallint null value
and nvl(name1,'FT')  =  'FT'  --test string null value
and nvl(name2,'FT')  =  'FT'  --test varchar(254) null value
and nvl(name3,'FT')  =  'FT'  --test char null value
and nvl(name4,2)  =  2  --test byte null value
and nvl(time1,date'2014-3-31')  =  date'2014-3-31'  --test date null value
and nvl(time2,timestamp'2019-08-08 16:33:41')  =  timestamp'2019-08-08 16:33:41'  --test timestamp null value
and nvl(flag,true)  =  true  --test boolean null value
