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

SELECT min(id1), max(id1),
       min(id2), max(id2),
       min(id3), max(id3),
       min(id4), max(id4),
       min(price1), max(price1),
       min(price2), max(price2),
       min(price3), max(price3),
       min(name1), max(name1),
       min(name2), max(name2),
       min(name3), max(name3),
       min(date1), max(date1),
       min(time1), max(time1),
       min(flag), max(flag),
       min(id_null), max(id_null),
       min(date_null), max(date_null),
       min(timestamp_null), max(timestamp_null),
       min(string_null), max(string_null),
       min(flag_null), max(flag_null),
       min(double_null), max(double_null)
FROM tdvt.test_measure
