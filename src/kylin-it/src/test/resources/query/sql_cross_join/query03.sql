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


SELECT Sta.cal_dt AS DAT, sum(Sta.PRICE) AS PRI, COUNT(*) AS COU
    FROM test_kylin_fact CALCS
    INNER JOIN
         (    SELECT cal_dt, PRICE
             FROM
              test_kylin_fact
              CROSS JOIN
             (SELECT 1) as temp
        ) as Sta
    ON CALCS.cal_dt = Sta.cal_dt
GROUP BY Sta.cal_dt
ORDER BY Sta.cal_dt
