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
package org.apache.kylin.query.util;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.tool.CalciteParser;

public class SqlNodeExtractor extends SqlBasicVisitor<SqlNode> {

    private List<SqlIdentifier> allSqlIdentifier = Lists.newArrayList();

    public static List<SqlIdentifier> getAllSqlIdentifier(String sql) throws SqlParseException {
        SqlNode parsed = CalciteParser.parse(sql);
        SqlNodeExtractor sqlNodeExtractor = new SqlNodeExtractor();
        parsed.accept(sqlNodeExtractor);
        return sqlNodeExtractor.allSqlIdentifier;
    }

    public static Map<SqlIdentifier, Pair<Integer, Integer>> getIdentifierPos(String sql) throws SqlParseException {
        List<SqlIdentifier> identifiers = getAllSqlIdentifier(sql);
        Map<SqlIdentifier, Pair<Integer, Integer>> identifierAndPositionMap = Maps.newHashMap();
        for (SqlIdentifier identifier : identifiers) {
            Pair<Integer, Integer> identifyPosition = CalciteParser.getReplacePos(identifier, sql);
            identifierAndPositionMap.put(identifier, identifyPosition);
        }
        return identifierAndPositionMap;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        allSqlIdentifier.add(id);
        return null;
    }
}
