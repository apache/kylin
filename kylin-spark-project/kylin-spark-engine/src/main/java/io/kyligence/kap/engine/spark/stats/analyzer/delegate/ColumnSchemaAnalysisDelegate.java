/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
