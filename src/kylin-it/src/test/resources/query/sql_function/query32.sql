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

select concat(ID1,name1),concat(ID2,name1),concat(ID3,name1),concat(ID4,name1),concat(price1,name1),concat(price2,name1),concat(price3,name1),concat(price5,name1),concat(price6,name1),concat(price7,name1),concat(name1,name1),concat(name2,name1),concat(name3,name1),concat(name4,name1),concat(time1,name1),concat(time2,name1),concat(flag,name1),
       concat(ID1,name2),concat(ID2,name2),concat(ID3,name2),concat(ID4,name2),concat(price1,name2),concat(price2,name2),concat(price3,name2),concat(price5,name2),concat(price6,name2),concat(price7,name2),concat(name1,name2),concat(name2,name2),concat(name3,name2),concat(name4,name2),concat(time1,name2),concat(time2,name2),concat(flag,name2),
       concat(ID1,'A'),concat(ID2,'A'),concat(ID3,'A'),concat(ID4,'A'),concat(price1,'A'),concat(price2,'A'),concat(price3,'A'),concat(price5,'A'),concat(price6,'A'),concat(price7,'A'),concat(name1,'A'),concat(name2,'A'),concat(name3,'A'),concat(name4,'A'),concat(time1,'A'),concat(time2,'A'),concat(flag,'A'),

       concat(name1,ID1),concat(name1,ID2),concat(name1,ID3),concat(name1,ID4),concat(name1,price1),concat(name1,price2),concat(name1,price3),concat(name1,price5),concat(name1,price6),concat(name1,price7),concat(name1,name1),concat(name1,name2),concat(name1,name3),concat(name1,name4),concat(name1,time1),concat(name1,time2),concat(name1,flag),
       concat(name2,ID1),concat(name2,ID2),concat(name2,ID3),concat(name2,ID4),concat(name2,price1),concat(name2,price2),concat(name2,price3),concat(name2,price5),concat(name2,price6),concat(name2,price7),concat(name2,name1),concat(name2,name2),concat(name2,name3),concat(name2,name4),concat(name2,time1),concat(name2,time2),concat(name2,flag),
       concat('A',ID1),concat('A',ID2),concat('A',ID3),concat('A',ID4),concat('A',price1),concat('A',price2),concat('A',price3),concat('A',price5),concat('A',price6),concat('A',price7),concat('A',name1),concat('A',name2),concat('A',name3),concat('A',name4),concat('A',time1),concat('A',time2),concat('A',flag)
from TEST_MEASURE
