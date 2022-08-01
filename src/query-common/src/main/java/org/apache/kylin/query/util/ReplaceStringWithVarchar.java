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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceStringWithVarchar implements KapQueryUtil.IQueryTransformer {
    private static final Logger logger = LoggerFactory.getLogger(ReplaceStringWithVarchar.class);
    private static final String REPLACED = "STRING";
    private static final String REPLACER = "VARCHAR";

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        try {
            SqlNode sqlNode = CalciteParser.parse(sql, project);
            SqlStringTypeCapturer visitor = new SqlStringTypeCapturer();
            sqlNode.accept(visitor);
            List<SqlNode> stringTypeNodes = visitor.getStringTypeNodes();
            String result = sql;
            for (SqlNode node : stringTypeNodes) {
                result = replaceStringWithVarchar(node, result);

            }
            return result;
        } catch (Exception e) {
            logger.error("replace `STRING` with `VARCHAR` error: ", e);
            return sql;
        }
    }

    private String replaceStringWithVarchar(SqlNode sqlNode, String sql) {
        Pair<Integer, Integer> startEndPos = CalciteParser.getReplacePos(sqlNode, sql);
        StringBuilder result = new StringBuilder(sql);
        result.replace(startEndPos.getFirst(), startEndPos.getSecond(), REPLACER);
        return result.toString();
    }

    static class SqlStringTypeCapturer extends SqlBasicVisitor<SqlNode> {
        List<SqlNode> stringTypeNodes = new ArrayList<>();

        @Override
        public SqlNode visit(SqlDataTypeSpec sqlDataTypeSpec) {
            String names = sqlDataTypeSpec.getTypeName().names.get(0);
            if (REPLACED.equalsIgnoreCase(names))
                stringTypeNodes.add(sqlDataTypeSpec);
            return null;
        }

        public List<SqlNode> getStringTypeNodes() {
            stringTypeNodes.sort((o1, o2) -> {
                SqlParserPos pos1 = o1.getParserPosition();
                SqlParserPos pos2 = o2.getParserPosition();
                int line1 = pos1.getLineNum();
                int line2 = pos2.getLineNum();
                int col1 = pos1.getColumnNum();
                int col2 = pos2.getColumnNum();
                if (line1 > line2)
                    return -1;
                else if (line1 < line2)
                    return 1;
                else
                    return col2 - col1;
            });
            return stringTypeNodes;
        }
    }
}
