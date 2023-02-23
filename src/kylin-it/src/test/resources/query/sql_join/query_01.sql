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

-- example: both table are full load, result have two context
--       join
--      /   \
--    join   A
--   /    \
--  A      B

SELECT buyer_account.account_country AS b_country
FROM test_account buyer_account
	JOIN test_country buyer_country ON buyer_account.account_country = buyer_country.country
	JOIN test_account seller_account ON buyer_account.account_country = seller_account.account_country
	ORDER BY b_country
LIMIT 100
