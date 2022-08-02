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
select CAL_DT,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01']) as first_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-02']) as second_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-03']) as third_day,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02']) as retention_oneday,
intersect_count(TEST_COUNT_DISTINCT_BITMAP, CAL_DT, array[date'2012-01-01',date'2012-01-02',date'2012-01-03']) as retention_twoday
from test_kylin_fact
where CAL_DT in (date'2012-01-01',date'2012-01-02',date'2012-01-03')
group by CAL_DT

