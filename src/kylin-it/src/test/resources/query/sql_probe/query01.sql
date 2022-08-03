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

SELECT
cast(2+2 as double) AS NUM4
, MIN(({fn YEAR(CURRENT_DATE)} - 1)) AS "TEMP_Calculation_718042704579547142__1060872050__0_"
, cast(null as double) AS NULL1
, YEAR(CURRENT_DATE) - 1 AS YEAR1
, MIN({fn MONTH(CURRENT_DATE)}) AS "TEMP_Calculation_718042704579547142__1229742960__0_"
, MIN(({fn YEAR(CURRENT_DATE)} - 1)) AS "TEMP_Calculation_718042704579547142__2383436532__0_"
, count(*) COU
from test_kylin_fact
