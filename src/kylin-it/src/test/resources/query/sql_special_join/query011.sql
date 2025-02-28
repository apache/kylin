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

SELECT COUNT("T"."ACCOUNT_COUNTRY"), "ACCOUNT_SELLER_LEVEL"
FROM (
	SELECT "PRICE", "TRANS_ID", "SELLER_ID" FROM "TEST_KYLIN_FACT" ORDER BY "TRANS_ID" DESC
	)
"S" INNER JOIN (
	SELECT "ACCOUNT_COUNTRY", "ACCOUNT_ID", "ACCOUNT_SELLER_LEVEL" FROM "TEST_ACCOUNT" ORDER BY "ACCOUNT_ID" ASC
	)
"T"
ON "T"."ACCOUNT_ID" = "S"."SELLER_ID"
INNER JOIN "TEST_COUNTRY" "E"
ON "T"."ACCOUNT_COUNTRY" = "E"."COUNTRY"
GROUP BY "ACCOUNT_SELLER_LEVEL"
ORDER BY ACCOUNT_SELLER_LEVEL
