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
-- test is false, is true, is not false, is not true

SELECT trans_id,
CASE WHEN IS_EFFECTUAL IS FALSE THEN 0 ELSE 1 END,
CASE WHEN IS_EFFECTUAL IS NOT TRUE THEN 0 ELSE 1 END
FROM TEST_KYLIN_FACT
WHERE (PRICE > 0) IS TRUE AND (ITEM_COUNT > 0) IS NOT FALSE

