--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
