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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.ImmutableList;

import lombok.val;

public class SqlIdentifierFormatterVisitor extends SqlBasicVisitor<Void> {
    private final String expr;
    private final Map<String, Set<String>> table2cols = Maps.newHashMap();
    private final Map<String, Set<String>> col2tables = Maps.newHashMap();

    public SqlIdentifierFormatterVisitor(String expr, List<NDataModel.NamedColumn> fullQualifiedNamedColumns) {
        this.expr = expr;
        for (val col : fullQualifiedNamedColumns) {
            if (col.getStatus() == NDataModel.ColumnStatus.TOMB) {
                continue;
            }
            String aliasDotColumn = col.getAliasDotColumn();
            String[] nameParts = aliasDotColumn.split("\\.");
            if (nameParts.length < 2) {
                throw new KylinException(ServerErrorCode.INVALID_MODEL_TYPE,
                        "Found invalid stored full qualified column name for " + nameParts[nameParts.length - 1]);
            }
            String table = nameParts[nameParts.length - 2];
            table2cols.putIfAbsent(table, Sets.newHashSet());
            String column = nameParts[nameParts.length - 1];
            Set<String> cols = table2cols.get(table);
            if (cols.contains(column)) {
                throw new KylinException(ServerErrorCode.DUPLICATED_COLUMN_NAME,
                        String.format("Found duplicate stored column %s for table %s!", column, table));
            }
            cols.add(column);
            col2tables.putIfAbsent(column, Sets.newHashSet());
            col2tables.get(column).add(table);
        }
    }

    @Override
    public Void visit(SqlIdentifier id) {
        if (id.names.size() == 1) {
            String column = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            Set<String> targetTbls = col2tables.getOrDefault(column, Sets.newHashSet());
            if (targetTbls.size() != 1) {
                throw new KylinException(ServerErrorCode.COLUMN_NOT_EXIST,
                        String.format(
                        "Found unrecognized or ambiguous column: %s in candidate tables [%s] in expression '%s'.",
                        id, targetTbls.stream().reduce(", ", String::join), expr));
            }
            id.names = ImmutableList.of(targetTbls.iterator().next(), column);
        } else if (id.names.size() >= 2) {
            String table = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            String column = id.names.get(1).toUpperCase(Locale.ROOT).trim();
            Set<String> cols = table2cols.get(table);
            // TODO: Support 3 or more name parts, like: database.table.name?
            if (id.names.size() > 2 || cols == null || !cols.contains(column)) {
                throw new KylinException(ServerErrorCode.COLUMN_NOT_EXIST,
                        String.format("Found unrecognized column: %s in expression '%s'.", id, expr));
            }
            id.names = ImmutableList.of(table, column);
        }
        return null;
    }

    @Override
    public Void visit(SqlCall call) {
        if (call instanceof SqlBasicCall
                && (call.getOperator() instanceof SqlAsOperator || call.getOperator() instanceof SqlAggFunction)) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        String.format("Unsupported SqlNode %s in expression %s", call, expr));
        }
        return super.visit(call);
    }
}
