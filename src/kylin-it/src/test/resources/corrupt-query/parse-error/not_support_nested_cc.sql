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

select
	seller_id,
	sum(case
		when seller_id > 10000000 and seller_id < 10000200 then pp
		else 0
	end) / sum(pp) as pp_ratio
from
	(
		select price * (1 - price) * (1-price) as pp from test_kylin_fact
		where price > 12 and cal_dt between '2012-01-01' and '2012-02-01'
	) as test_fact
group by
	seller_id
order by
	seller_id;
