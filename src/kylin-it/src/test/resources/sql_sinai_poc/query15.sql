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

SELECT COUNT(DISTINCT "X_XXXXXXXX_XX_XX"."XXXXXX_XX") AS "ctd_XXXXXX_XX_ok",
       SUM({fn CONVERT(0, SQL_BIGINT)}) AS "sum_Calculation_336925569152049156_ok"
FROM "XXXXXXXXX_XXXXXXXXX"."X_XXXXXXXX_XX_XX" "X_XXXXXXXX_XX_XX"
WHERE ("X_XXXXXXXX_XX_XX"."FULL_NAME" = 'MUKVIN XU')
HAVING (COUNT(1) > 0);
