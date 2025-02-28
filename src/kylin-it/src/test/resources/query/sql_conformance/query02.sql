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
SELECT CAL_DT,
             CASE WHEN SELLER_ID = '057|001|02|021' THEN '编辑' WHEN SELLER_ID = '058|001|02|021' THEN '趣美颜' ELSE 'AI抠图' END AS SELLER_ID,
                                                                                                                            sum(PRICE) AS p
      FROM TEST_KYLIN_FACT
      WHERE SELLER_ID IN (1,2,3)
      GROUP BY CAL_DT,
               CASE WHEN SELLER_ID = '057|001|02|021' THEN '编辑' WHEN SELLER_ID = '058|001|02|021' THEN '趣美颜' ELSE 'AI抠图' END
