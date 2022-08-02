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

WITH AAA AS
(
SELECT
 LSTG_FORMAT_NAME "针梭织__0",
 COUNT(DISTINCT CASE WHEN TRANS_ID = 100 THEN LSTG_FORMAT_NAME ELSE NULL END) "封样合格率（款）__MAX__1",
 SUM(TRANS_ID) "实际收货配套累计量__SUM__2",
 SUM(CASE WHEN ORDER_ID > 200 AND ORDER_ID > 200 THEN 0 ELSE TRANS_ID END),
 0 "__GROUPING_ID"
 FROM
(
 SELECT
LSTG_FORMAT_NAME
,ORDER_ID
,TRANS_ID
,CAL_DT
 FROM
TEST_KYLIN_FACT
) TT
 WHERE TT.LSTG_FORMAT_NAME != '1'
 GROUP BY LSTG_FORMAT_NAME
 )
,
CCC
AS
(SELECT 'AA' "针梭织__0",
0.1 "封样合格率（款）__MAX__1",
0.2 "实际收货配套累计量__SUM__2",
33 "实际收货配套累计占比__MAX__3",
1 "__GROUPING_ID"
)

SELECT AAA. * FROM AAA UNION ALL SELECT CCC. * FROM CCC
LIMIT 500
