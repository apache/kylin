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


SELECT cast(id1 AS double ),
       cast(id2 AS float),
       cast(id3 AS bigint),
       cast(id4 AS int),
       cast(price1 AS float),
       cast(price2 AS double),
       cast(price3 AS decimal(19,6)),
       cast(price5 AS double ),
       cast(price6 AS tinyint),
       cast(price7 AS smallint),
       cast(name1 AS varchar),
       cast(name2 AS varchar(254)),
       cast(name3 AS char),
       cast(name4 AS tinyint),
       cast(time1 AS date),
       cast(time2 AS TIMESTAMP),
       cast(flag AS boolean)
FROM test_measure
