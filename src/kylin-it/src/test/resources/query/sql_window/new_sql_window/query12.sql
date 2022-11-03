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
select * from(
  select cal_dt, lstg_format_name, sum(price) as GMV,
  round(100*sum(price)/first_value(sum(price)) over (partition by lstg_format_name,SLR_SEGMENT_CD order by cast(cal_dt as timestamp) range interval '1' day preceding) , 0) as "last_day",
  round(first_value(sum(price)) over (partition by lstg_format_name,SLR_SEGMENT_CD order by cast(cal_dt as timestamp) range cast(1 as interval day) preceding),0) as "last_year" from test_kylin_fact
  where cal_dt between '2013-01-08' and '2013-01-15' or cal_dt between '2013-01-07' and '2013-01-15' or cal_dt between '2012-01-01' and '2012-01-15'
  group by cal_dt, lstg_format_name,SLR_SEGMENT_CD
)t
where cal_dt between '2013-01-06' and '2013-01-15'
