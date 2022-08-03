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
select TIMESTAMPDIFF(day, date'2018-01-01', date'2018-10-10'),
        TIMESTAMPDIFF(day, timestamp'2018-01-01 00:00:00', date'2018-10-10'),
        TIMESTAMPDIFF(day, timestamp'2018-01-01 00:00:00', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(day, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(year, date'2017-01-01', timestamp'2018-01-01 00:00:00'),
        TIMESTAMPDIFF(quarter, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(month, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(week, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(hour, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(minute, date'2018-01-01', timestamp'2018-10-10 00:00:00'),
        TIMESTAMPDIFF(second, date'2018-01-01', timestamp'2018-10-10 00:00:00')
