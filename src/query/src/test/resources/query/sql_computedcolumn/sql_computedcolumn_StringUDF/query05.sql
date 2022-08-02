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
select concat_ws(';',collect_set(ID1+1)),
       concat_ws(';',collect_set(ID2+1)),
       concat_ws(',',collect_set(ID3+1)),
       concat_ws('_',collect_set(concat(name1,name1))),
       concat_ws(' ',collect_set(concat(name2,name2))),
       concat_ws(';',collect_set(concat(name3,name3))),
       concat_ws(';',collect_set(concat(name4,name4))),
       concat_ws('-',collect_set(price1+1)),
       concat_ws(';',collect_set(price2+1)),
       concat_ws(';',collect_set(price3+1)),
       concat_ws(';',collect_set(price5+1)),
       concat_ws(';',collect_set(price6+1)),
       concat_ws(';',collect_set(price7+1)),
       concat_ws(';',collect_set(concat(time1,1))),
       concat_ws(';',collect_set(concat(time2,1))),
       concat_ws(';',collect_set(concat(flag,1)))
from test_measure
limit 10