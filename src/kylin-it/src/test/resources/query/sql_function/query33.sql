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
select concat_ws(';',collect_set(ID1)),
       concat_ws(';',collect_set(ID2)),
       concat_ws(',',collect_set(ID3)),
       concat_ws('_',collect_set(name1)),
       concat_ws(' ',collect_set(name2)),
       concat_ws(';',collect_set(name3)),
       concat_ws(';',collect_set(name4)),
       concat_ws('-',collect_set(price1)),
       concat_ws(';',collect_set(price2)),
       concat_ws(';',collect_set(price3)),
       concat_ws(';',collect_set(price5)),
       concat_ws(';',collect_set(price6)),
       concat_ws(';',collect_set(price7)),
       concat_ws(';',collect_set(time1)),
       concat_ws(';',collect_set(time2)),
       concat_ws(';',collect_set(flag))
from test_measure
limit 10