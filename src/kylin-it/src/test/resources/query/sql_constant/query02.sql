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
  trunc('2009-02-12', 'MM'),trunc('2015-10-27', 'YEAR'),
  trunc(date'2009-02-12', 'MM'),trunc(timestamp'2009-02-12 00:00:00', 'MM'),
  add_months('2016-08-31', 1),add_months(date'2016-08-31', 2),add_months(timestamp'2016-08-31 00:00:00', 1),
  date_add('2016-07-30', 1),date_add(date'2016-07-30', 1),date_add(timestamp'2016-07-30 00:00:00', 1),
  date_sub('2016-07-30', 1),date_sub(date'2016-07-30', 1),date_sub(timestamp'2016-07-30 00:00:00', 1),
  from_unixtime(0, 'yyyy-MM-dd HH:mm:ss'),
  from_utc_timestamp('2016-08-31', 'Asia/Seoul'),from_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),from_utc_timestamp(date'2016-08-31', 'Asia/Seoul'),
  months_between('1997-02-28 10:30:00', '1996-10-30'),months_between(timestamp'1997-02-28 10:30:00', date'1996-10-30'),
  to_utc_timestamp('2016-08-31', 'Asia/Seoul'),to_utc_timestamp(timestamp'2016-08-31 00:00:00', 'Asia/Seoul'),to_utc_timestamp(date'2016-08-31', 'Asia/Seoul')


