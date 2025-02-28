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

SELECT {fn TIMESTAMPADD(SQL_TSI_MONTH,1,{fn TIMESTAMPADD(SQL_TSI_FRAC_SECOND,({fn CONVERT(NULL, SQL_DOUBLE)} * 24 * 60 * 60 - {fn CONVERT({fn TRUNCATE({fn CONVERT(NULL, SQL_DOUBLE)} * 24 * 60 * 60,0)}, SQL_BIGINT)})*1000000000,{fn TIMESTAMPADD(SQL_TSI_SECOND,{fn CONVERT({fn TRUNCATE(({fn CONVERT(NULL, SQL_DOUBLE)} * 24 * 60 - {fn CONVERT({fn TRUNCATE({fn CONVERT(NULL, SQL_DOUBLE)} * 24 * 60,0)}, SQL_BIGINT)}) * 60,0)}, SQL_BIGINT)},{fn TIMESTAMPADD(SQL_TSI_MINUTE,{fn CONVERT({fn TRUNCATE(({fn CONVERT(NULL, SQL_DOUBLE)} * 24 - {fn CONVERT({fn TRUNCATE({fn CONVERT(NULL, SQL_DOUBLE)} * 24,0)}, SQL_BIGINT)}) * 60,0)}, SQL_BIGINT)},{fn TIMESTAMPADD(SQL_TSI_HOUR,{fn CONVERT({fn TRUNCATE(({fn CONVERT(NULL, SQL_DOUBLE)} - {fn CONVERT({fn TRUNCATE({fn CONVERT(NULL, SQL_DOUBLE)},0)}, SQL_BIGINT)}) * 24,0)}, SQL_BIGINT)},{fn TIMESTAMPADD(SQL_TSI_DAY,{fn CONVERT({fn TRUNCATE({fn CONVERT(NULL, SQL_DOUBLE)},0)}, SQL_BIGINT)},{ts '1900-01-01 00:00:00'})})})})})})} AS "TEMP_Test__2095510626__0_",
  1 AS "X__alias__0"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY 1