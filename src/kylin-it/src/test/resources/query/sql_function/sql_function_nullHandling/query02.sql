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

select *
from TEST_MEASURE
where ifnull(ID2,132322342)  =  132322342  --test bigint null value
and ifnull(ID3,14123)  =  14123  --test long null value
and ifnull(ID4,313)  =  313  --test int null value
and ifnull(price1,12.34)  =  12.34  --test float null value
and ifnull(price2,124.44)  =  124.44  --test double null value
and ifnull(price3,14.242343)  =  14.242343  --test decimal(19,6) null value
and ifnull(price5,2)  =  2  --test short null value
and ifnull(price6,7)  =  7  --test tinyint null value
and ifnull(price7,1)  =  1  --test smallint null value
and ifnull(name1,'FT')  =  'FT'  --test string null value
and ifnull(name2,'FT')  =  'FT'  --test varchar(254) null value
and ifnull(name3,'FT')  =  'FT'  --test char null value
and ifnull(name4,2)  =  2  --test byte null value
and ifnull(time1,date'2014-3-31')  =  date'2014-3-31'  --test date null value
and ifnull(time2,timestamp'2019-08-08 16:33:41')  =  timestamp'2019-08-08 16:33:41'  --test timestamp null value
and ifnull(flag,true)  =  true  --test boolean null value
and nvl(ID2,132322342)  =  132322342  --test bigint null value
and nvl(ID3,14123)  =  14123  --test long null value
and nvl(ID4,313)  =  313  --test int null value
and nvl(price1,12.34)  =  12.34  --test float null value
and nvl(price2,124.44)  =  124.44  --test double null value
and nvl(price3,14.242343)  =  14.242343  --test decimal(19,6) null value
and nvl(price5,2)  =  2  --test short null value
and nvl(price6,7)  =  7  --test tinyint null value
and nvl(price7,1)  =  1  --test smallint null value
and nvl(name1,'FT')  =  'FT'  --test string null value
and nvl(name2,'FT')  =  'FT'  --test varchar(254) null value
and nvl(name3,'FT')  =  'FT'  --test char null value
and nvl(name4,2)  =  2  --test byte null value
and nvl(time1,date'2014-3-31')  =  date'2014-3-31'  --test date null value
and nvl(time2,timestamp'2019-08-08 16:33:41')  =  timestamp'2019-08-08 16:33:41'  --test timestamp null value
and nvl(flag,true)  =  true  --test boolean null value