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

package org.apache.kylin.metadata.query;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class QueryHistorySqlTest {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    @Test
    public void testGetSqlWithParameterBindingComment() {
        final String SQL = "select col1, col2, col3 from table1 where col1 = ? and col2 = ?";
        QueryHistorySql queryHistorySql = new QueryHistorySql(SQL, null, null);

        assertEquals(SQL, queryHistorySql.getSqlWithParameterBindingComment());

        List<QueryHistorySqlParam> params = new ArrayList<>();
        params.add(new QueryHistorySqlParam(1, "java.lang.Integer", "INTEGER", "1001"));
        params.add(new QueryHistorySqlParam(2, "java.lang.String", "VARCHAR", "Male"));
        queryHistorySql.setParams(params);

        final String SQL_WITH_PARAMS_COMMENT = SQL
                + LINE_SEPARATOR
                + LINE_SEPARATOR
                + "-- [PARAMETER BINDING]"
                + LINE_SEPARATOR
                + "-- Binding parameter [1] as [INTEGER] - [1001]"
                + LINE_SEPARATOR
                + "-- Binding parameter [2] as [VARCHAR] - [Male]"
                + LINE_SEPARATOR
                + "-- [PARAMETER BINDING END]";
        assertEquals(SQL_WITH_PARAMS_COMMENT, queryHistorySql.getSqlWithParameterBindingComment());
    }
}
