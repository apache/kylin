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

-- KE-19732

select floor(cal_dt to day), ceil(cal_dt to day),
floor(cal_dt to week), ceil(cal_dt to week),
floor(cal_dt to month), ceil(cal_dt to month),
floor(cal_dt to quarter), ceil(cal_dt to quarter),
floor(cal_dt to year), ceil(cal_dt to year),
floor(date'2020-11-17' TO day), ceil(date'2020-11-17' TO day),
floor(date'2020-11-17' TO WEEK), ceil(date'2020-11-17' TO WEEK),
floor(date'2020-11-17' TO month), ceil(date'2020-11-17' TO month),
floor(date'2020-11-17' TO quarter), ceil(date'2020-11-17' TO quarter),
floor(date'2020-11-17' TO year), ceil(date'2020-11-17' TO year)
from test_kylin_fact
