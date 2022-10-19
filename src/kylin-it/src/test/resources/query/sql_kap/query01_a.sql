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


-- It's converted from: SELECT LSTG_FORMAT_NAME, {fn RIGHT(LSTG_FORMAT_NAME, {fn CONVERT(1, SQL_BIGINT)})}
-- #5170 handle complex expression in {fn RIGHT(...)}
SELECT LSTG_FORMAT_NAME, {fn RIGHT(LSTG_FORMAT_NAME, {fn CONVERT(1, SQL_BIGINT)})} as LAST,
 {fn LEFT(LSTG_FORMAT_NAME, {fn CONVERT(1, SQL_BIGINT)})} as FIRST
FROM TEST_KYLIN_FACT
GROUP BY LSTG_FORMAT_NAME
