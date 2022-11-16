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
package io.kyligence.kap.clickhouse.ddl;

import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.visitor.DefaultSQLRender;

public class ClickHouseRender extends DefaultSQLRender {

    @Override
    public void visit(CreateTable<?> createTable) {
        ClickHouseCreateTable query = (ClickHouseCreateTable) createTable;

        if (query.createTableWithColumns()) {
            super.visit(query);
        } else {
            createTablePrefix(query);
            result.append(' ')
                    .append(DefaultSQLRender.KeyWord.AS);
            acceptOrVisitValue(query.likeTable());
        }
        if (query.engine() !=null) {
            result.append(' ')
                    .append(KeyWord.ENGINE)
                    .append(" = ")
                    .append(query.engine());
        }
        if (query.partitionBy() != null) {
            result.append(' ')
                    .append(KeyWord.PARTITION_BY)
                    .append(' ').append("`").append(query.partitionBy()).append("`");
        }
        if (query.createTableWithColumns()) {
            result.append(' ').append(DefaultSQLRender.KeyWord.ORDER_BY).append(' ').append(KeyWord.TUPLE);
            result.append('(');
            result.append(query.orderBy().stream().map(c -> "`" + c + "`").collect(Collectors.joining(",")));
            result.append(')');
        }
        if (!query.getTableSettings().isEmpty()) {
            result.append(' ').append(KeyWord.SETTINGS).append(' ');
            result.append(query.getTableSettings().entrySet().stream()
                    .map(setting -> setting.getKey().toSql(setting.getValue())).collect(Collectors.joining(",")));
        }
    }

    @Override
    public void visit(AlterTable alterTable) {
        result.append(DefaultSQLRender.KeyWord.ALTER).append(' ')
                .append(DefaultSQLRender.KeyWord.TABLE);
        acceptOrVisitValue(alterTable.getTable());
        result.append(' ');
        if (alterTable.isFreeze()) {
            result.append(' ').append(KeyWord.FREEZE);
        } else if (alterTable.getManipulatePartition() != null) {
            acceptOrVisitValue(alterTable.getManipulatePartition());
        } else if (alterTable.getAttachPart() != null) {
            result.append(' ').append(KeyWord.ATTACH_PART).append(' ')
                    .append('\'').append(alterTable.getAttachPart()).append('\'');
        } else if (alterTable.getManipulateIndex() != null) {
            acceptOrVisitValue(alterTable.getManipulateIndex());
        } else if (alterTable.getModifyColumn() != null) {
            acceptOrVisitValue(alterTable.getModifyColumn());
        }
    }

    @Override
    public void visit(AlterTable.ManipulatePartition manipulatePartition) {
        result.append(manipulatePartition.getPartitionOperation().getOperation())
                .append(' ')
                .append(KeyWord.PARTITION)
                .append(' ')
                .append('\'').append(manipulatePartition.getPartition()).append("'");
        if (manipulatePartition.getDestTable() != null) {
            result.append(' ').append(DefaultSQLRender.KeyWord.TO)
                    .append(' ')
                    .append(DefaultSQLRender.KeyWord.TABLE);
            acceptOrVisitValue(manipulatePartition.getDestTable());
        }
    }

    @Override
    public void visit(AlterTable.ManipulateIndex manipulateIndex) {
        result.append(manipulateIndex.getIndexOperation().toString()).append(' ').append(KeyWord.INDEX).append(' ')
                .append(manipulateIndex.getName());

        if (AlterTable.IndexOperation.ADD == manipulateIndex.getIndexOperation()) {
            result.append(' ').append('`').append(manipulateIndex.getColumn()).append('`').append(' ')
                    .append(KeyWord.TYPE).append(' ').append(manipulateIndex.getExpr()).append(' ')
                    .append(KeyWord.GRANULARITY).append(' ').append(manipulateIndex.getGranularity());
        }
    }

    @Override
    public void visit(AlterTable.ModifyColumn modifyColumn) {
        result.append(KeyWord.MODIFY_COLUMN)
                .append(' ')
                .append(modifyColumn.getColumn())
                .append(' ')
                .append(modifyColumn.getDatatype());
    }

    private static class KeyWord {
        public static final String PARTITION_BY = "PARTITION BY";
        private static final String ENGINE = "ENGINE";
        private static final String TUPLE = "tuple";
        private static final String PARTITION = "PARTITION";
        private static final String FREEZE = "FREEZE";
        private static final String ATTACH_PART = "ATTACH PART";
        private static final String SETTINGS = "SETTINGS";
        private static final String INDEX = "INDEX";
        private static final String TYPE = "TYPE";
        private static final String GRANULARITY = "GRANULARITY";
        private static final String MODIFY_COLUMN = "MODIFY COLUMN";

        private KeyWord() {
        }
    }
}
