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


select to_char(CAL_DT,'YEAR'),
       to_char(CAL_DT,'y'),
	   to_char(CAL_DT,'Y'),
	   to_char(CAL_DT,'MONTH'),
	   to_char(CAL_DT,'M'),
	   to_char(CAL_DT,'DAY'),
	   to_char(CAL_DT,'D'),
       to_char(CAL_DT,'d'),
       to_char(CAL_DT,'H'),
	   to_char(CAL_DT,'h'),
	   to_char(CAL_DT,'m'),
	   to_char(CAL_DT,'s')
from TEST_KYLIN_FACT
