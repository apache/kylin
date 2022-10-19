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
    COUNT(CONCAT("LSTG_FORMAT_NAME", "LSTG_FORMAT_NAME")),
    count(substr(LSTG_FORMAT_NAME,2)),
    COUNT(INSTR("LSTG_FORMAT_NAME", "LSTG_FORMAT_NAME")) "INSTRa",
    COUNT(INSTR("LSTG_FORMAT_NAME", "LSTG_FORMAT_NAME", 1)) "INSTRb",
    COUNT(LENGTH("LSTG_FORMAT_NAME")),
    COUNT(STRPOS("LSTG_FORMAT_NAME", "LSTG_FORMAT_NAME")),
    count(DISTINCT substr(LSTG_FORMAT_NAME,2)),
    count(DISTINCT concat(LSTG_FORMAT_NAME,LSTG_FORMAT_NAME)),
    count(DISTINCT instr(LSTG_FORMAT_NAME,LSTG_FORMAT_NAME)) "INSTRc",
    count(DISTINCT length(LSTG_FORMAT_NAME))
from test_kylin_fact
