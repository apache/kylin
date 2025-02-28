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

select *
from test_kylin_fact
where (lstg_format_name, test_count_distinct_bitmap)
    not in (
            ('FP-GTC', 'TEST1'),
            ('FP-GTC', 'TEST2'),
            ('FP-GTC', 'TEST3'),
            ('FP-GTC', 'TEST4'),
            ('FP-GTC', 'TEST5'),
            ('FP-GTC', 'TEST421'),
            ('FP-GTC', 'TEST703'),
            ('FP-GTC', 'TEST303'),
            ('FP-GTC', 'TEST755'),
            ('FP-GTC', 'TEST851'),
            ('FP-GTC', 'TEST204'),
            ('FP-GTC', 'TEST29'),
            ('FP-GTC', 'TEST658'),
            ('FP-GTC', 'TEST405'),
            ('FP-GTC', 'TEST16'),
            ('FP-GTC', 'TEST17'),
            ('FP-GTC', 'TEST18'),
            ('FP-GTC', 'TEST19'),
            ('FP-GTC', 'TEST20'),
            ('FP-GTC', 'TEST520'),
            ('FP-GTC', 'TEST521'),
            ('FP-GTC', 'TEST522'),
            ('Auction', 'TEST134'),
            ('Auction', 'TEST415'),
            ('Auction', 'TEST876')
          )
   or lstg_format_name in ('ABIN')
order by trans_id limit 30;
