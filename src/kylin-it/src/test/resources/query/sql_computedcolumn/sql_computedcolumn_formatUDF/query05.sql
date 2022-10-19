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

select count(distinct TO_TIMESTAMP("UPD_DATE")) "NoFmt",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm:ss')) "yyyy-MM-dd HH:mm:ss",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm')) "yyyy-MM-dd HH:mm",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH')) "yyyy-MM-dd HH",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd')) "yyyy-MM-dd",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM')) "yyyy-MM",
       count(distinct To_Timestamp(UPD_DATE,'yyyy')) "yyyy"
from TEST_CATEGORY_GROUPINGS
