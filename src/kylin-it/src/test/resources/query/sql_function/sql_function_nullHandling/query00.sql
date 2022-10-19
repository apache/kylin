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


select ifnull(LSTG_FORMAT_NAME,'sorry, name is null'),
       ifnull(TRANS_ID,0),
       ifnull(PRICE,0),
       ifnull(CAL_DT,CAL_DT),
       nvl(LSTG_FORMAT_NAME,'sorry, name is null'),
       nvl(TRANS_ID,0),
       nvl(PRICE,0),
       nvl(CAL_DT,CAL_DT),
       isnull(LSTG_FORMAT_NAME),
       isnull(TRANS_ID),
       isnull(PRICE),
       isnull(CAL_DT)
from TEST_KYLIN_FACT;
