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

-- AL-717
WITH A AS (SELECT LSTG_FORMAT_NAME, COUNT(1) FROM TEST_KYLIN_FACT GROUP BY LSTG_FORMAT_NAME ),
B AS ( SELECT A.*,EXPLODE(ARRAY(1,2,3)) AS DIM FROM A),
C AS (  SELECT DIM_A,SUM(DIM_B) FROM (SELECT DIM AS DIM_A,
            CASE WHEN DIM=1 THEN 10 WHEN DIM=2 THEN 20 WHEN DIM=3 THEN 30 END AS DIM_B FROM B)
            GROUP BY DIM_A)
SELECT * FROM C
