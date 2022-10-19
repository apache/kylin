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


SELECT *
    FROM
        (SELECT *
        FROM TEST_KYLIN_FACT KYLIN_FACT
        WHERE KYLIN_FACT.CAL_DT BETWEEN date'2021-06-25' AND date'2021-07-01' ) KYLIN_FACT
        INNER JOIN
            (SELECT ACCOUNT_ID
            FROM TEST_ACCOUNT
            WHERE ACCOUNT_CONTACT = 'abc'
            ) u
                ON u.ACCOUNT_ID = KYLIN_FACT.LSTG_SITE_ID
        LEFT JOIN TEST_ORDER
            ON KYLIN_FACT.ORDER_ID = TEST_ORDER.ORDER_ID
        LEFT JOIN TEST_ACCOUNT us
            ON us.ACCOUNT_ID = KYLIN_FACT.LSTG_SITE_ID
        LEFT JOIN EDW.TEST_SITES
            ON EDW.TEST_SITES.SITE_ID = us.ACCOUNT_BUYER_LEVEL

