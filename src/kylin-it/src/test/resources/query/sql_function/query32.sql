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
select concat(ID1,name1),concat(ID2,name1),concat(ID3,name1),concat(ID4,name1),concat(price1,name1),concat(price2,name1),concat(price3,name1),concat(price5,name1),concat(price6,name1),concat(price7,name1),concat(name1,name1),concat(name2,name1),concat(name3,name1),concat(name4,name1),concat(time1,name1),concat(time2,name1),concat(flag,name1),
       concat(ID1,name2),concat(ID2,name2),concat(ID3,name2),concat(ID4,name2),concat(price1,name2),concat(price2,name2),concat(price3,name2),concat(price5,name2),concat(price6,name2),concat(price7,name2),concat(name1,name2),concat(name2,name2),concat(name3,name2),concat(name4,name2),concat(time1,name2),concat(time2,name2),concat(flag,name2),
       concat(ID1,'A'),concat(ID2,'A'),concat(ID3,'A'),concat(ID4,'A'),concat(price1,'A'),concat(price2,'A'),concat(price3,'A'),concat(price5,'A'),concat(price6,'A'),concat(price7,'A'),concat(name1,'A'),concat(name2,'A'),concat(name3,'A'),concat(name4,'A'),concat(time1,'A'),concat(time2,'A'),concat(flag,'A'),

       concat(name1,ID1),concat(name1,ID2),concat(name1,ID3),concat(name1,ID4),concat(name1,price1),concat(name1,price2),concat(name1,price3),concat(name1,price5),concat(name1,price6),concat(name1,price7),concat(name1,name1),concat(name1,name2),concat(name1,name3),concat(name1,name4),concat(name1,time1),concat(name1,time2),concat(name1,flag),
       concat(name2,ID1),concat(name2,ID2),concat(name2,ID3),concat(name2,ID4),concat(name2,price1),concat(name2,price2),concat(name2,price3),concat(name2,price5),concat(name2,price6),concat(name2,price7),concat(name2,name1),concat(name2,name2),concat(name2,name3),concat(name2,name4),concat(name2,time1),concat(name2,time2),concat(name2,flag),
       concat('A',ID1),concat('A',ID2),concat('A',ID3),concat('A',ID4),concat('A',price1),concat('A',price2),concat('A',price3),concat('A',price5),concat('A',price6),concat('A',price7),concat('A',name1),concat('A',name2),concat('A',name3),concat('A',name4),concat('A',time1),concat('A',time2),concat('A',flag)
from TEST_MEASURE