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
select
  trim(BOTH '' from '') as R1
, trim(BOTH 'a' from '') as R2
, trim(BOTH '' from 'a') as R3
, trim(BOTH 'a' from 'a') as R4
, trim(BOTH 'aa' from 'aa') as R5
, trim(BOTH 'aaa' from 'aa') as R6
, trim(BOTH 'aaa' from 'aaa123aaa') as R7
, trim(BOTH 'aaa' from '123aaa') as R8
, trim(BOTH 'aaa' from 'aaa123') as R9
, trim(BOTH 'ab' from 'abab123abab') as R10
, trim(BOTH 'ab' from '123abab') as R11
, trim(BOTH 'ab' from 'abab123') as R12

, trim(LEADING '' from '') as R13
, trim(LEADING 'a' from '') as R14
, trim(LEADING '' from 'a') as R15
, trim(LEADING 'a' from 'a') as R16
, trim(LEADING 'aa' from 'aa') as R17
, trim(LEADING 'aaa' from 'aa') as R18
, trim(LEADING 'aaa' from 'aaa123aaa') as R19
, trim(LEADING 'aaa' from '123aaa') as R20
, trim(LEADING 'aaa' from 'aaa123') as R21
, trim(LEADING 'ab' from 'abab123abab') as R22
, trim(LEADING 'ab' from '123abab') as R23
, trim(LEADING 'ab' from 'abab123') as R24

, trim(TRAILING '' from '') as R25
, trim(TRAILING 'a' from '') as R26
, trim(TRAILING '' from 'a') as R27
, trim(TRAILING 'a' from 'a') as R28
, trim(TRAILING 'aa' from 'aa') as R29
, trim(TRAILING 'aaa' from 'aa') as R30
, trim(TRAILING 'aaa' from 'aaa123aaa') as R31
, trim(TRAILING 'aaa' from '123aaa') as R32
, trim(TRAILING 'aaa' from 'aaa123') as R33
, trim(TRAILING 'ab' from 'abab123abab') as R34
, trim(TRAILING 'ab' from '123abab') as R35
, trim(TRAILING 'ab' from 'abab123') as R36

, trim(BOTH '' from LSTG_FORMAT_NAME) as R37
, trim(BOTH ' ' from LSTG_FORMAT_NAME) as R38
, trim(BOTH 'AB' from LSTG_FORMAT_NAME) as R39
, trim(LEADING 'ABIN' from LSTG_FORMAT_NAME) as R40
, trim(LEADING 'AB' from LSTG_FORMAT_NAME) as R41
, trim(TRAILING 'IN' from LSTG_FORMAT_NAME) as R42
, trim(TRAILING 'ABIN' from LSTG_FORMAT_NAME) as R43

, trim(cast(null as varchar)) as R44

, trim(BOTH '' from cast(null as varchar)) as R45
, trim(LEADING '' from cast(null as varchar)) as R46
, trim(TRAILING '' from cast(null as varchar)) as R47

, trim(BOTH 'a' from cast(null as varchar)) as R48
, trim(LEADING 'a' from cast(null as varchar)) as R49
, trim(TRAILING 'a' from cast(null as varchar)) as R50

, trim(BOTH cast(null as varchar) from cast(null as varchar)) as R51
, trim(LEADING cast(null as varchar) from cast(null as varchar)) as R52
, trim(TRAILING cast(null as varchar) from cast(null as varchar)) as R53
, trim(' abc ') as R54
  from TEST_KYLIN_FACT where LSTG_FORMAT_NAME = 'ABIN' limit 1