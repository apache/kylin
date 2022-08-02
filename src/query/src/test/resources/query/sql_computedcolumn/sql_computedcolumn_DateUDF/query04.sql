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
	    count(DATE_TRUNC('YEAR', "CAL_DT")),
	    count(DATE_TRUNC('YYYY', "CAL_DT")),
	    count(DATE_TRUNC('YY', "CAL_DT")),
	    count(DATE_TRUNC('MM', "CAL_DT")),
	    count(DATE_TRUNC('MONTH', "CAL_DT")),
	    count(DATE_TRUNC('DAY', "CAL_DT")),
	    count(DATE_TRUNC('DD', "CAL_DT")),
	    count(DATE_TRUNC('HOUR', "CAL_DT")),
	    count(DATE_TRUNC('WEEK', "CAL_DT")),

	    count(distinct DATE_TRUNC('YEAR', "CAL_DT")),
	    count(distinct DATE_TRUNC('YYYY', "CAL_DT")),
	    count(distinct DATE_TRUNC('YY', "CAL_DT")),
	    count(distinct DATE_TRUNC('MM', "CAL_DT")),
	    count(distinct DATE_TRUNC('MONTH', "CAL_DT")),
	    count(distinct DATE_TRUNC('DAY', "CAL_DT")),
	    count(distinct DATE_TRUNC('DD', "CAL_DT")),
	    count(distinct DATE_TRUNC('HOUR', "CAL_DT")),
	    count(distinct DATE_TRUNC('WEEK', "CAL_DT"))
from TEST_KYLIN_FACT