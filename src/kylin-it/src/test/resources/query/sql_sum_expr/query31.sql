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


-- ISSUE https://olapio.atlassian.net/browse/KE-8892

SELECT count(SLR_SEGMENT_CD) AS COL1,
    SUM(CASE WHEN TEST_KYLIN_FACT.CAL_DT BETWEEN '2011-10-01' AND '2012-10-31' THEN PRICE ELSE 0 END) as COL2
from TEST_KYLIN_FACT
WHERE LSTG_FORMAT_NAME='FP-GTC'
