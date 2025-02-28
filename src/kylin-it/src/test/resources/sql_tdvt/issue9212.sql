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


select DATETIME0,DATETIME1
from TDVT.CALCS
where 1=1
and cast(DATETIME0 as date) >= date'2004-07-28'
and cast(DATETIME1 as timestamp) >= timestamp'2004-07-28 12:34:00'
and date'2004-07-28'<= cast(DATETIME0 as date)
and timestamp'2004-07-28 12:34:00' <= cast(DATETIME1 as timestamp)
group by DATETIME0,DATETIME1
