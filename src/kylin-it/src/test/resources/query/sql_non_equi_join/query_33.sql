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

SELECT  COUNT(*) AS CNT
FROM
(
select  PRICE as LFNAME1 from TEST_KYLIN_FACT
) as t1
INNER JOIN
(
select  PRICE as LFNAME2 from TEST_KYLIN_FACT
) as t2
on t1.LFNAME1 = t2.LFNAME2
OR ((t1.LFNAME1 is null) AND (t2.LFNAME2 is null))
