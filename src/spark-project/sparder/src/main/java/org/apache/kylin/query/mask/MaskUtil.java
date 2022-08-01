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

package org.apache.kylin.query.mask;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.query.util.EscapeDialect;
import org.apache.kylin.query.util.EscapeParser;
import org.apache.kylin.query.util.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

class MaskUtil {

    private MaskUtil() {
    }

    static Dataset<Row> dFToDFWithIndexedColumns(Dataset<Row> df) {
        String[] indexedColNames = new String[df.columns().length];
        for (int i = 0; i < indexedColNames.length; i++) {
            indexedColNames[i] = df.columns()[i].replaceAll("[`.]", "_") + "_" + i;
        }
        return df.toDF(indexedColNames);
    }

    static List<SqlIdentifier> getCCCols(String ccExpr) {
        SqlParser.ConfigBuilder parserBuilder = SqlParser.configBuilder().setQuoting(Quoting.BACK_TICK);
        String selectSql = "select " + ccExpr;
        EscapeParser parser = new EscapeParser(EscapeDialect.CALCITE, selectSql);
        try {
            selectSql = parser.Input();
        } catch (ParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR, "Failed to convert column expr " + ccExpr, e);
        }
        SqlParser sqlParser = SqlParser.create(selectSql, parserBuilder.build());
        SqlSelect select;
        try {
            select = (SqlSelect) sqlParser.parseQuery();
        } catch (SqlParseException e) {
            throw new KylinException(QueryErrorCode.FAILED_PARSE_ERROR,
                    "Failed to parse computed column expr " + ccExpr, e);
        }
        return select.getSelectList().getList().stream().flatMap(op -> getSqlIdentifiers(op).stream())
                .collect(Collectors.toList());
    }

    static List<SqlIdentifier> getSqlIdentifiers(SqlNode sqlNode) {
        List<SqlIdentifier> ids = new ArrayList<>();
        if (sqlNode instanceof SqlIdentifier) {
            ids.add((SqlIdentifier) sqlNode);
            return ids;
        } else if (sqlNode instanceof SqlCall) {
            return ((SqlCall) sqlNode).getOperandList().stream().flatMap(op -> getSqlIdentifiers(op).stream())
                    .collect(Collectors.toList());
        } else {
            return ids;
        }
    }
}
