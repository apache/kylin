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
    COUNT(*) AS TOTAL
FROM
    (
        SELECT
            D1,
            D2,
            D3
        FROM
            (
                SELECT
                    T.LO_ORDERKEY AS D1,
                    T.LO_CUSTKEY AS D2,
                    T1.C_CUSTKEY AS D3
                FROM
                    P_LINEORDER AS T
                        LEFT JOIN CUSTOMER T1 ON T.LO_CUSTKEY = T1.C_CUSTKEY
                WHERE
                        (SELECT 2) IN (T.LO_CUSTKEY, T1.C_CUSTKEY)
                GROUP BY
                    T.LO_ORDERKEY,
                    T.LO_CUSTKEY,
                    T1.C_CUSTKEY
            )
    )
