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

SELECT PRICE AS p, 0 as a FROM TEST_KYLIN_FACT  as t1 where 1=2
UNION ALL
SELECT PRICE AS p, 0 as a FROM TEST_KYLIN_FACT  as t2 where 1=1
UNION ALL
SELECT 1 AS p, PRICE AS a FROM TEST_KYLIN_FACT as t3 where 1=1
ORDER BY p, a
limit 500
