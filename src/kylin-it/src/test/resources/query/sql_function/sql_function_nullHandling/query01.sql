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

select ifnull(ID2,132322342),  --test bigint null value
       ifnull(ID3,14123),  --test long null value
       ifnull(ID4,313),  --test int null value
       ifnull(price1,12.34),  --test float null value
       ifnull(price2,124.44),  --test double null value
       ifnull(price3,14.242343),  --test decimal(19,6) null value
       ifnull(price5,2),  --test short null value
       ifnull(price6,7),  --test tinyint null value
       ifnull(price7,1),  --test smallint null value
       ifnull(name1,'FT'),  --test string null value
       ifnull(name2,'FT'),  --test varchar(254) null value
       ifnull(name3,'FT'),  --test char null value
       ifnull(name4,2),  --test byte null value
       ifnull(time1,date'2014-3-31'),  --test date null value
       ifnull(time2,timestamp'2019-08-08 16:33:41.061'),  --test timestamp null value
       ifnull(flag,true), --test boolean null value
       nvl(ID2,132322342),  --test bigint null value
       nvl(ID3,14123),  --test long null value
       nvl(ID4,313),  --test int null value
       nvl(price1,12.34),  --test float null value
       nvl(price2,124.44),  --test double null value
       nvl(price3,14.242343),  --test decimal(19,6) null value
       nvl(price5,2),  --test short null value
       nvl(price6,7),  --test tinyint null value
       nvl(price7,1),  --test smallint null value
       nvl(name1,'FT'),  --test string null value
       nvl(name2,'FT'),  --test varchar(254) null value
       nvl(name3,'FT'),  --test char null value
       nvl(name4,2),  --test byte null value
       nvl(time1,date'2014-3-31'),  --test date null value
       nvl(time2,timestamp'2019-08-08 16:33:41.061'),  --test timestamp null value
       nvl(flag,true),
       isnull(ID2),  --test bigint null value
       isnull(ID3),  --test long null value
       isnull(ID4),  --test int null value
       isnull(price1),  --test float null value
       isnull(price2),  --test double null value
       isnull(price3),  --test decimal(19,6) null value
       isnull(price5),  --test short null value
       isnull(price6),  --test tinyint null value
       isnull(price7),  --test smallint null value
       isnull(name1),  --test string null value
       isnull(name2),  --test varchar(254) null value
       isnull(name3),  --test char null value
       isnull(name4),  --test byte null value
       isnull(time1),  --test date null value
       isnull(time2),  --test timestamp null value
       isnull(flag) --test boolean null value
from TEST_MEASURE

