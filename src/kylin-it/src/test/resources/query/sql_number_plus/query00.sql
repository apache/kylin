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

select sum((cast((TIMESTAMPDIFF(day, TIMESTAMPADD(DAY, cast(mod(
 (8 - (mod(
 (dayofweek(cast(cast(FLOOR(TIMESTAMPADD(DAY, cast((0 - ((mod(
 (dayofweek(cast('2023-12-11 00:00:00' as timestamp)) + 5),
 7
 ) + 1) - 1)) as integer), cast('2023-12-11 00:00:00' as timestamp)) to year) as date) as date)) + 5),
 7
 ) + 1)),
 7
) as integer), cast(cast(FLOOR(TIMESTAMPADD(DAY, cast((0 - ((mod(
 (dayofweek(cast('2023-12-11 00:00:00' as timestamp)) + 5),
 7
) + 1) - 1)) as integer), cast('2023-12-11 00:00:00' as timestamp)) to year) as date) as date)), cast('2023-12-11 00:00:00' as timestamp)) / 7) as integer) + 1)) "__fcol_0"
LIMIT 500