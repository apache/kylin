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


-- KE-27687
select
repeat(NAME1,2),
repeat(NAME2,3),
repeat(NAME3,4),
repeat(NAME3,0),
repeat(NAME3,-1),
repeat(concat(NAME1,NAME1),2),
repeat(concat(NAME2,NAME2),3),
repeat(concat(NAME3,NAME3),4),
repeat(concat(NAME3,NAME3),0),
repeat(concat(NAME3,NAME3),-1)
from TEST_MEASURE
