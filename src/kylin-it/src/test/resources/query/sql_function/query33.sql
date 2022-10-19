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

select concat_ws(';',collect_set(ID1)),
       concat_ws(';',collect_set(ID2)),
       concat_ws(',',collect_set(ID3)),
       concat_ws('_',collect_set(name1)),
       concat_ws(' ',collect_set(name2)),
       concat_ws(';',collect_set(name3)),
       concat_ws(';',collect_set(name4)),
       concat_ws('-',collect_set(price1)),
       concat_ws(';',collect_set(price2)),
       concat_ws(';',collect_set(price3)),
       concat_ws(';',collect_set(price5)),
       concat_ws(';',collect_set(price6)),
       concat_ws(';',collect_set(price7)),
       concat_ws(';',collect_set(time1)),
       concat_ws(';',collect_set(time2)),
       concat_ws(';',collect_set(flag))
from test_measure
limit 10
