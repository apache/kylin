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

 -- related https://github.com/Kyligence/KAP/issues/7614

SELECT
    t1.leaf_categ_id, COUNT(*) AS nums
FROM
    (SELECT
        f.leaf_categ_id
    FROM
        test_kylin_fact f inner join TEST_CATEGORY_GROUPINGS o on f.leaf_categ_id = o.leaf_categ_id and f.LSTG_SITE_ID = o.site_id
    WHERE
        f.lstg_format_name = 'ABIN') t1
    INNER JOIN
    (SELECT
        leaf_categ_id
    FROM
        test_kylin_fact f
    INNER JOIN test_order o ON f.order_id = o.order_id
    WHERE
        buyer_id > 100) t2 ON t1.leaf_categ_id = t2.leaf_categ_id
GROUP BY t1.leaf_categ_id
ORDER BY nums, leaf_categ_id
limit 100