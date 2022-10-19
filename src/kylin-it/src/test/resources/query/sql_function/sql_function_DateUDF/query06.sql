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
	   DATE_TRUNC('YEAR', "CAL_DT"),
	   DATE_TRUNC('YYYY', "CAL_DT"),
	   DATE_TRUNC('YY', "CAL_DT"),
	   DATE_TRUNC('MM', "CAL_DT"),
	   DATE_TRUNC('MONTH', "CAL_DT"),
	   DATE_TRUNC('DAY', "CAL_DT"),
	   DATE_TRUNC('DD', "CAL_DT"),
	   DATE_TRUNC('HOUR', "CAL_DT"),
	   DATE_TRUNC('WEEK', "CAL_DT")
from TEST_KYLIN_FACT
