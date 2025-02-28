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


SELECT D_DATEKEY,
       DATE_TIME,
       count(*),
       sum(D_YEAR) sy
FROM (SELECT 19950301 AS DATE_TIME
      UNION ALL
      SELECT 19950302 AS DATE_TIME
      UNION ALL
      SELECT 19950303 AS DATE_TIME) t1
         LEFT JOIN SSB.DATES t2 ON t2.D_DATEKEY <= t1.DATE_TIME
    AND t2.D_DATEKEY >= cast(CONCAT(SUBSTR(cast(t1.DATE_TIME as string), 1, 6), '01') as integer)
GROUP BY D_DATEKEY,
         DATE_TIME
