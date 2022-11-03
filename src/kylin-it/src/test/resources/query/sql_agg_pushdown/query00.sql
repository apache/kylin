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

SELECT "自定义 SQL 查询"."CAL_DT" ,
       SUM ("自定义 SQL 查询"."SELLER_ID") AS "TEMP_Calculation_54915774428294",
               COUNT (DISTINCT "自定义 SQL 查询"."CAL_DT") AS "TEMP_Calculation_97108873613918",
                     COUNT (DISTINCT (CASE
                                          WHEN ("t0"."x_measure__0" > 0) THEN "t0"."LSTG_FORMAT_NAME"
                                          ELSE CAST (NULL AS VARCHAR (1))
                                      END)) AS "TEMP_Calculation_97108873613911"
FROM
  (SELECT *
   FROM TEST_KYLIN_FACT) "自定义 SQL 查询"
INNER JOIN
     (SELECT LSTG_FORMAT_NAME, ORDER_ID, SUM ("PRICE") AS "X_measure__0"
      FROM TEST_KYLIN_FACT  GROUP  BY LSTG_FORMAT_NAME, ORDER_ID) "t0" ON "自定义 SQL 查询"."ORDER_ID" = "t0"."ORDER_ID"
GROUP  BY "自定义 SQL 查询"."CAL_DT"
