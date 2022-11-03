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

-- ISSUE #3269

SELECT "t2"."X_measure__1" AS "X_measure__1",
       "t1"."X_measure__3" AS "X_measure__3",
       "t4"."X_measure__5" AS "X_measure__5"
FROM
  (SELECT round(AVG("t0"."X_measure__2"), 0) AS "X_measure__3"
   FROM
     (SELECT SELLER_ID,
             round(SUM(PRICE), 0) AS "X_measure__2"
      FROM TEST_KYLIN_FACT
      GROUP BY SELLER_ID) "t0" WHERE "t0"."X_measure__2" > 0 HAVING (COUNT(1) > 0)) "t1"
CROSS JOIN
  (SELECT round(SUM(PRICE), 0) AS "X_measure__1"
   FROM TEST_KYLIN_FACT HAVING (COUNT(1) > 0)) "t2"
CROSS JOIN
  (SELECT round(AVG("t3"."X_measure__4"), 0) AS "X_measure__5"
   FROM
     (SELECT SELLER_ID,
             round(SUM(PRICE), 0) AS "X_measure__4"
      FROM TEST_KYLIN_FACT
      GROUP BY SELLER_ID) "t3" WHERE "t3"."X_measure__4" > 0 HAVING (COUNT(1) > 0)) "t4"