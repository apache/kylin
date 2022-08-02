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
SELECT
  (
    CASE
      WHEN ("t0"."X_measure__0" < 164000) THEN '[0,164000)'
      WHEN ("t0"."X_measure__0" < 166000) THEN '[164000, 166000)'
      ELSE '[166000, +∞'
    END
  ) AS "Calculation_1162491713472385025",
  SUM("TEST_KYLIN_FACT"."PRICE") AS "usr_GMV_SUM_ok"
FROM
  "DEFAULT"."TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
  INNER JOIN (
    SELECT
      "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" AS "LSTG_FORMAT_NAME",
      SUM("TEST_KYLIN_FACT"."PRICE") AS "X_measure__0"
    FROM
      "DEFAULT"."TEST_KYLIN_FACT" "TEST_KYLIN_FACT"
    WHERE
      ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" NOT IN ('Auction'))
    GROUP BY
      "TEST_KYLIN_FACT"."LSTG_FORMAT_NAME"
  ) "t0" ON (
    ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" = "t0"."LSTG_FORMAT_NAME")
  )
WHERE
  ("TEST_KYLIN_FACT"."LSTG_FORMAT_NAME" NOT IN ('Auction'))
GROUP BY
  (
    CASE
      WHEN ("t0"."X_measure__0" < 164000) THEN '[0,164000)'
      WHEN ("t0"."X_measure__0" < 166000) THEN '[164000, 166000)'
      ELSE '[166000, +∞'
    END
  )