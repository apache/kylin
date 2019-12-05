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

package io.kyligence.kap.engine.spark.stats.analyzer.delegate;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

public class ColumnSchemaAnalysisDelegate extends AbstractColumnAnalysisDelegate<ColumnSchemaAnalysisDelegate> {

    public final String[] HIVE_KEYWORD = new String[] { "ALL", "ALTER", "AND", "ARRAY", "AS", "AUTHORIZATION",
            "BETWEEN", "BIGINT", "BINARY", "BOOLEAN", "BOTH", "BY", "CASE", "CAST", "CHAR", "COLUMN", "CONF", "CREATE",
            "CROSS", "CUBE", "CURRENT", "CURRENT_DATE", "CURRENT_TIMESTAMP", "CURSOR", "DATABASE", "DATE", "DECIMAL",
            "DELETE", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "EXCHANGE", "EXISTS", "EXTENDED",
            "EXTERNAL", "FALSE", "FETCH", "FLOAT", "FOLLOWING", "FOR", "FROM", "FULL", "FUNCTION", "GRANT", "GROUP",
            "GROUPING", "HAVING", "IF", "IMPORT", "IN", "INNER", "INSERT", "INT", "INTERSECT", "INTERVAL", "INTO", "IS",
            "JOIN", "LATERAL", "LEFT", "LESS", "LIKE", "LOCAL", "MACRO", "MAP", "MORE", "NONE", "NOT", "NULL", "OF",
            "ON", "OR", "ORDER", "OUT", "OUTER", "OVER", "PARTIALSCAN", "PARTITION", "PERCENT", "PRECEDING", "PRESERVE",
            "PROCEDURE", "RANGE", "READS", "REDUCE", "REVOKE", "RIGHT", "ROLLUP", "ROW", "ROWS", "SELECT", "SET",
            "SMALLINT", "TABLE", "TABLESAMPLE", "THEN", "TIMESTAMP", "TO", "TRANSFORM", "TRIGGER", "TRUE", "TRUNCATE",
            "UNBOUNDED", "UNION", "UNIQUEJOIN", "UPDATE", "USER", "USING", "UTC_TMESTAMP", "VALUES", "VARCHAR", "WHEN",
            "WHERE", "WINDOW", "WITH" };

    private boolean once = true;

    private boolean illegal = false;

    public ColumnSchemaAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    public void analyze(Row row, Object colValue) {
        if (once) {
            // check schema
            if (columnDesc.getName().startsWith("_")
                    || ArrayUtils.contains(HIVE_KEYWORD, columnDesc.getName().toUpperCase())) {
                illegal = true;
            }

            once = false;
        }
    }

    @Override
    protected ColumnSchemaAnalysisDelegate doReduce(ColumnSchemaAnalysisDelegate another) {
        this.illegal = (this.isIllegal() || another.isIllegal());
        return this;
    }

    public boolean isIllegal() {
        return illegal;
    }
}
