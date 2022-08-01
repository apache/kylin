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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.alias.ExpressionComparator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelViewSqlNodeComparator extends ExpressionComparator.SqlNodeComparator {

    private final NDataModel model;

    public ModelViewSqlNodeComparator(NDataModel model) {
        this.model = model;
    }

    @Override
    protected boolean isSqlIdentifierEqual(SqlIdentifier querySqlIdentifier, SqlIdentifier exprSqlIdentifier) {
        if (querySqlIdentifier.isStar()) {
            return exprSqlIdentifier.isStar();
        } else if (exprSqlIdentifier.isStar()) {
            return false;
        }

        try {
            // 1. the query col table is not really matter here
            // as all columns are supposed to be selected from the single view table underneath
            // 2. and col renaming is not supported now in cc converting
            // so we use the col name in the query directly
            String queryCol = null;
            if (querySqlIdentifier.names.size() == 1) {
                queryCol = querySqlIdentifier.names.get(0);
            } else if (querySqlIdentifier.names.size() == 2) {
                queryCol = querySqlIdentifier.names.get(1);
            }

            NDataModel.NamedColumn modelCol = model.getColumnByColumnNameInModel(queryCol);
            String modelColTableAlias = modelCol.getAliasDotColumn().split("\\.")[0];
            String modelColName = modelCol.getAliasDotColumn().split("\\.")[1];

            return StringUtils.equalsIgnoreCase(modelColTableAlias, exprSqlIdentifier.names.get(0))
                    && StringUtils.equalsIgnoreCase(modelColName, exprSqlIdentifier.names.get(1));
        } catch (NullPointerException | IllegalStateException e) {
            log.trace("met exception when doing expressions[{}, {}] comparison", querySqlIdentifier, exprSqlIdentifier,
                    e);
            return false;
        }
    }
}
