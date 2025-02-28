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

SELECT {fn CONVERT({fn TRUNCATE({fn TIMESTAMPDIFF(SQL_TSI_DAY,{fn TIMESTAMPADD(SQL_TSI_DAY,(-1 * ({fn MOD((7 + {fn DAYOFWEEK({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)})} - 2), 7)})),{fn CONVERT({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)}, SQL_DATE)})},{fn TIMESTAMPADD(SQL_TSI_DAY,(-1 * ({fn MOD((7 + {fn DAYOFWEEK("CALCS"."DATETIME0")} - 2), 7)})),{fn CONVERT("CALCS"."DATETIME0", SQL_DATE)})})} / 7,0)}, SQL_BIGINT)} AS "TEMP_Test__110287328__0_"
FROM "TDVT"."CALCS" "CALCS"
GROUP BY {fn CONVERT({fn TRUNCATE({fn TIMESTAMPDIFF(SQL_TSI_DAY,{fn TIMESTAMPADD(SQL_TSI_DAY,(-1 * ({fn MOD((7 + {fn DAYOFWEEK({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)})} - 2), 7)})),{fn CONVERT({fn CONVERT("CALCS"."DATE2", SQL_TIMESTAMP)}, SQL_DATE)})},{fn TIMESTAMPADD(SQL_TSI_DAY,(-1 * ({fn MOD((7 + {fn DAYOFWEEK("CALCS"."DATETIME0")} - 2), 7)})),{fn CONVERT("CALCS"."DATETIME0", SQL_DATE)})})} / 7,0)}, SQL_BIGINT)}