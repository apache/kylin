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

-- #ISSUE 6546 test cast nullable value in bigdecimal/date/timestamp
select lstg_format_name
, floor(bigint0)+1.1 bigint0
, cast(bigint0 as decimal)-1.1 bigint1
, cast(double0 as decimal)*1.1 double1, cast(decimal0 as decimal)*1.1 decimal1
, cast(DAT0 as timestamp) + interval '1' day as DAT1
, cast(TIME0 as date) - interval '1' day as TIME1
from
    (    select lstg_format_name
                , cast(2 as bigint) bigint0, cast(2 as double) double0, cast(2 as decimal) decimal0
                , date'1971-01-01' DAT0
                , timestamp'1999-01-01 01:01:01' TIME0
                from test_kylin_fact
                group by lstg_format_name
                except all
                select 'extra', cast(null as bigint),cast(null as double),cast(null as decimal)
                , cast(null as date),cast(null as timestamp)
    ) as tmp