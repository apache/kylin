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
package org.apache.kylin.common.persistence.metadata.mapper;

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;

public final class ComputeColumnDynamicSqlSupport {

    public static final ComputeColumn sqlTable = new ComputeColumn();

    private ComputeColumnDynamicSqlSupport(){
    }

    public static final class ComputeColumn extends BasicSqlTable<ComputeColumn> {

        public final SqlColumn<String> columnName = column("column_name", JDBCType.VARCHAR);

        public final SqlColumn<String> tableIdentity = column("table_identity", JDBCType.VARCHAR);

        public final SqlColumn<String> tableAlias = column("table_alias", JDBCType.VARCHAR);

        public final SqlColumn<String> expression = column("expression", JDBCType.VARCHAR);

        public final SqlColumn<String> innerExpression = column("inner_expression", JDBCType.VARCHAR);

        public final SqlColumn<String> datatype = column("datatype", JDBCType.VARCHAR);

        public final SqlColumn<Integer> referenceCount = column("reference_count", JDBCType.INTEGER);

        public final SqlColumn<String> ccComment = column("cc_comment", JDBCType.VARCHAR);

        public final SqlColumn<String> expressionMd5 = column("expression_md5", JDBCType.VARCHAR);

        public ComputeColumn() {
            super("compute_column", ComputeColumn::new);
        }
    }
}
