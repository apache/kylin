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


select upper(lstg_format_name) as lstg_format_name, count(*) as cnt from test_kylin_fact
where lower(lstg_format_name)='abin' and substring(lstg_format_name,1,3) in ('ABI') and upper(lstg_format_name) > 'AAAA' and
lower(lstg_format_name) like '%b%' and char_length(lstg_format_name) < 10 and char_length(lstg_format_name) > 3 and 'abc'||lstg_format_name||'a'||'b'||'c'='abcABINabc'
group by lstg_format_name
