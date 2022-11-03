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
       ceil(time2 TO quarter),floor(time2 TO quarter),
       ceil(time2 TO year),floor(time2 TO year),
       ceil(time2 to month),floor(time2 to month),
       ceil(time2 to day),floor(time2 to day),
       ceil(time2 to week),floor(time2 to week),
       ceil(time2 to HOUR),floor(time2 to HOUR),
       ceil(time2 to MINUTE),floor(time2 to MINUTE),
       ceil(time2 to SECOND),floor(time2 to SECOND),
       floor(floor(time2 to HOUR) to HOUR),
       ceil(ceil(time2 to HOUR) to HOUR),
       floor(ceil(time2 to HOUR) to HOUR),
       ceil(floor(time2 to HOUR) to HOUR),
       floor(floor(time2 to day) to HOUR),
       ceil(ceil(time2 to day) to HOUR),
       floor(ceil(time2 to day) to HOUR),
       ceil(floor(time2 to day) to HOUR)
from test_measure