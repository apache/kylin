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

--test to_char(timestamp,part)
select to_char(TEST_TIME_ENC,'YEAR'),
       to_char(TEST_TIME_ENC,'Y'),
       to_char(TEST_TIME_ENC,'y'),
       to_char(TEST_TIME_ENC,'MONTH'),
       to_char(TEST_TIME_ENC,'M'),
       to_char(TEST_TIME_ENC,'DAY'),
       to_char(TEST_TIME_ENC,'D'),
       to_char(TEST_TIME_ENC,'d'),
       to_char(TEST_TIME_ENC,'HOUR'),
       to_char(TEST_TIME_ENC,'H'),
       to_char(TEST_TIME_ENC,'h'),
       to_char(TEST_TIME_ENC,'MINUTE'),
       to_char(TEST_TIME_ENC,'MINUTES'),
       to_char(TEST_TIME_ENC,'m'),
       to_char(TEST_TIME_ENC,'SECOND'),
       to_char(TEST_TIME_ENC,'SECONDS'),
       to_char(TEST_TIME_ENC,'s')
from TEST_ORDER