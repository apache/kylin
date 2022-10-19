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
    ceil(timestamp '2012-03-31 23:59:59.888' to second ),
    ceil(timestamp '2012-03-31 23:59:59.888' to minute ),
    ceil(ceil(timestamp '2012-03-31 23:59:59.888' to hour ) to minute ),
    ceil(timestamp '2012-03-31 23:59:59.888' to hour ),
    ceil(ceil(timestamp '2012-03-31 23:59:59.888' to hour ) to hour),
    ceil(timestamp '2012-03-31 00:01:59.12' to day ),
    ceil(timestamp '2012-02-29 00:00:00.0' to day ),
    ceil(timestamp '2012-12-31 23:59:59.888' to month ),
    ceil(timestamp '2012-12-31 23:59:59.888' to year ),
    --ceil(ceil(date '2013-03-31' to HOUR) to HOUR), -- calcite not support ceil(date_type to timeunit)
    ceil(timestamp '2013-03-31 00:00:00' to hour ),
    floor(timestamp '2013-03-31 00:00:00' to hour )
from test_measure limit 1
