/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class DDLCheckContextTest {

    @Test
    void testGetLogicalViewPersistSql() {
        {
            DDLCheckContext context = new DDLCheckContext("create logical view test123 as select * from ssb.lineorder",
                    "default", null, null, null, false);
            context.setCommandType(DDLConstant.CREATE_LOGICAL_VIEW);
            assertEquals("CREATE LOGICAL VIEW test123 as select * from ssb.lineorder",
                    context.getLogicalViewPersistSql());
        }

        {
            DDLCheckContext context = new DDLCheckContext("replace logical view test123 as select * from ssb.lineorder",
                    "default", null, null, null, false);
            context.setCommandType(DDLConstant.REPLACE_LOGICAL_VIEW);
            assertEquals("CREATE LOGICAL VIEW test123 as select * from ssb.lineorder",
                    context.getLogicalViewPersistSql());
        }

        {
            DDLCheckContext context = new DDLCheckContext("select * from ssb.lineorder", "default", null, null, null,
                    false);
            assertEquals("select * from ssb.lineorder", context.getLogicalViewPersistSql());
        }

        {
            DDLCheckContext context = new DDLCheckContext("drop logical view test123 as select * from ssb.lineorder",
                    "default", null, null, null, false);
            context.setCommandType(DDLConstant.DROP_LOGICAL_VIEW);
            assertEquals("drop logical view test123 as select * from ssb.lineorder",
                    context.getLogicalViewPersistSql());
        }
    }
}
