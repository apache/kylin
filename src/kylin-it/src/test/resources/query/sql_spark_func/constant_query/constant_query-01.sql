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
select BROUND(2.5, 0),
CBRT(27.0),
CONV(-10, 16, -10),CONV('100', 2, 10),
COSH(0),
EXPM1(0),
FACTORIAL(5),
HYPOT(3, 4),
LOG(10, 100),
LOG10(100),
LOG1P(0),
LOG2(2),
RINT(12.3456),
SINH(0),
TANH(0),
BASE64('Spark SQL'),
UNBASE64('U3BhcmsgU1FM'),
ENCODE('abc', 'utf-8'),
DECODE(ENCODE('abc', 'utf-8'), 'utf-8'),
FIND_IN_SET('ab','abc,b,ab,c,def'),
LCASE('SparkSql'),
UCASE('SparkSql'),
LEVENSHTEIN('kitten', 'sitting'),
LOCATE('bar', 'foobarbar'),LOCATE('bar', 'foobarbar', 5),
LPAD('hi', 5, '??'),
RPAD('hi', 5, '??'),
REPLACE('ABCabc', 'abc', 'DEF'),
RTRIM('KR', 'SPARK'),
SENTENCES('Hi there! Good morning.'),
SUBSTRING_INDEX('www.apache.org', '.', 1),
UNIX_TIMESTAMP('2016-04-08 09:00:00', 'yyyy-MM-dd HH:mm:ss')