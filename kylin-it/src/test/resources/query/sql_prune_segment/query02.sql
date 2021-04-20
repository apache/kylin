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

select  lo_orderdate, lo_quantity, sum(lo_revenue) from ssb.p_lineorder
where
(lo_orderdate = 19920906 and lo_quantity = 4) or
(lo_orderdate = 19920905 and lo_quantity = 9) and
(lo_orderdate = 19920904 and lo_quantity = 7) or
(
    lo_orderdate > 19920906 and lo_orderdate <= 19920907 and (lo_quantity = 6 or lo_quantity = 49)
)
group by lo_orderdate, lo_quantity
;{"scanRowCount":9,"scanBytes":0,"scanFiles":2,"cuboidId":[7],"exactlyMatched":[false]}