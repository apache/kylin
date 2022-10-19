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


select
CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)+223
    END                             AS  ACTIVE_ACCTS
,   CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)*556
    END                             AS  ACTIVE_PTYS
,   CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)+21121
    END AS EXP1
,   CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.CAL_DT)
    END                                 /NULLIF(
    CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.CAL_DT)+23333
    END
, 0) AS EXP2
,   CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)+332323232
    END/NULLIF(
    CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)*4
    END
, 0) AS EXP3
,   CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)*2
    END/NULLIF(
    CASE
        WHEN
            T4.CAL_DT   <   TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        THEN
            COUNT(T4.ITEM_COUNT)+1
    END
, 0) AS EXP4
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.CAL_DT)+11212211
    END                             AS  N_ACTIVE_ACCTS
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)
    END                             AS  N_ACTIVE_PTYS
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            SUM(T4.ITEM_COUNT)
    END AS EXP5
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)
    END                                 /NULLIF(
    CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)
    END
, 0) AS EXP6
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)
    END                                 /NULLIF(
    CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)*111
    END
, 0) AS EXP7
,   CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(MONTH, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)*11
    END                                 /NULLIF(
    CASE
        WHEN
            T4.CAL_DT   >=  TIMESTAMPADD(YEAR, -6, CAST('2018-10-22' AS  DATE))
        OR  T4.CAL_DT   IS  NULL
        THEN
            COUNT(T4.ITEM_COUNT)*55
    END
, 0) AS EXP8
FROM
    TEST_KYLIN_FACT  T4
LEFT JOIN
    TEST_KYLIN_FACT T2
ON
    T4.CAL_DT  =   T2.CAL_DT
    group by T4.CAL_DT
