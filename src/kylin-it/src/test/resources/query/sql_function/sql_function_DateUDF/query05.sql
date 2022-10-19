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
       ceil(time2 TO quarter),floor(time2 TO quarter),
       ceil(time2 TO year),floor(time2 TO year),
       ceil(time2 to month),floor(time2 to month),
       ceil(time2 to day),floor(time2 to day),
       ceil(time2 to week),floor(time2 to week),
       ceil(time2 to HOUR),floor(time2 to HOUR),
       ceil(time2 to MINUTE),floor(time2 to MINUTE),
       ceil(time2 to SECOND),floor(time2 to SECOND),
       floor(floor(time2 to HOUR) to HOUR),
       ceil(ceil(time2 to HOUR) to HOUR),
       floor(ceil(time2 to HOUR) to HOUR),
       ceil(floor(time2 to HOUR) to HOUR),
       floor(floor(time2 to day) to HOUR),
       ceil(ceil(time2 to day) to HOUR),
       floor(ceil(time2 to day) to HOUR),
       ceil(floor(time2 to day) to HOUR)
from test_measure
