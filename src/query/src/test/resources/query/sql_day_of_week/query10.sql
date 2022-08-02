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
select week('2012-01-01')
,week(cal_dt)
,week(cast(cal_dt as varchar))
,month('2012-01-01')
,month(cal_dt)
,month(cast(cal_dt as varchar))
,quarter('2012-01-01')
,quarter(cal_dt)
,quarter(cast(cal_dt as varchar))
,year('2012-01-01')
,year(cal_dt)
,year(cast(cal_dt as varchar))
,dayofyear('2012-01-01')
,dayofyear(cal_dt)
,dayofyear(cast(cal_dt as varchar))
,dayofmonth('2012-01-01')
,dayofmonth(cal_dt)
,dayofmonth(cast(cal_dt as varchar))
,dayofweek('2012-01-01')
,dayofweek(cal_dt)
,dayofweek(cast(cal_dt as varchar))
,dayofweek('2012-01-01 00:10:00')
,dayofweek(cast(cal_dt as timestamp))
from test_kylin_fact

