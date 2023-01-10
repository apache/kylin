/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
export const mockSQL = `SELECT
KYLIN_CAL_DT.CAL_DT as KYLIN_CAL_DT_CAL_DT
,KYLIN_ACCOUNT.ACCOUNT_ID as KYLIN_ACCOUNT_ACCOUNT_ID
,KYLIN_CAL_DT.YEAR_BEG_DT as KYLIN_CAL_DT_YEAR_BEG_DT
,KYLIN_ACCOUNT.ACCOUNT_COUNTRY as KYLIN_ACCOUNT_ACCOUNT_COUNTRY
,KYLIN_SALES.SELLER_ID as KYLIN_SALES_SELLER_ID
,KYLIN_SALES.PRICE as KYLIN_SALES_PRICE
,KYLIN_SALES.ITEM_COUNT as KYLIN_SALES_ITEM_COUNT
FROM DEFAULT.KYLIN_SALES as KYLIN_SALES
INNER JOIN DEFAULT.KYLIN_ACCOUNT as KYLIN_ACCOUNT
ON KYLIN_SALES.SELLER_ID = KYLIN_ACCOUNT.ACCOUNT_ID
INNER JOIN DEFAULT.KYLIN_CAL_DT as KYLIN_CAL_DT
ON KYLIN_SALES.PART_DT = KYLIN_CAL_DT.CAL_DT
WHERE 1=1 AND (KYLIN_SALES.PART_DT >= '2012-02-01' AND KYLIN_SALES.PART_DT < '2013-01-01')
;`
