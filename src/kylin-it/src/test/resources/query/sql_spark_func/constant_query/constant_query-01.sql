--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
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
