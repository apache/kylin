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
package org.apache.kylin.common.util;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.ImmutableList;

public class AddTableNameSqlVisitor extends SqlBasicVisitor<Object> {
    private static final String DEFAULT_REASON = "Something went wrong. %s";
    private String expr;
    private Map<String, String> colToTable;
    private Set<String> ambiguityCol;
    private Set<String> allColumn;

    public AddTableNameSqlVisitor(String expr, Map<String, String> colToTable, Set<String> ambiguityCol,
            Set<String> allColumn) {
        this.expr = expr;
        this.colToTable = colToTable;
        this.ambiguityCol = ambiguityCol;
        this.allColumn = allColumn;
    }

    @Override
    public Object visit(SqlIdentifier id) {
        boolean ok = true;
        if (id.names.size() == 1) {
            String column = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            if (!colToTable.containsKey(column) || ambiguityCol.contains(column)) {
                ok = false;
            } else {
                id.names = ImmutableList.of(colToTable.get(column), column);
            }
        } else if (id.names.size() == 2) {
            String table = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            String column = id.names.get(1).toUpperCase(Locale.ROOT).trim();
            ok = allColumn.contains(table + "." + column);
        } else {
            ok = false;
        }
        if (!ok) {
            throw new IllegalArgumentException(
                    "Unrecognized column: " + id.toString() + " in expression '" + expr + "'.");
        }
        return null;
    }

    @Override
    public Object visit(SqlCall call) {
        if (call instanceof SqlBasicCall) {
            if (call.getOperator() instanceof SqlAsOperator) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, DEFAULT_REASON, "null"));
            }

            if (call.getOperator() instanceof SqlAggFunction) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, DEFAULT_REASON, "null"));
            }
        }
        return call.getOperator().acceptCall(this, call);
    }
}
