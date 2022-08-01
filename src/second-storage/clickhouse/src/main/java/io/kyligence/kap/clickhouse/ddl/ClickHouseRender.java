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
            result.append(' ')
                    .append(DefaultSQLRender.KeyWord.ORDER_BY)
                    .append(' ')
                    .append(KeyWord.TUPLE)
                    .append('(')
                    .append(String.join(",", query.orderBy()))
                    .append(')');
        }
        if (query.getDeduplicationWindow() > 0) {
            result.append(' ')
                    .append(KeyWord.SETTINGS)
                    .append(' ')
                    .append(KeyWord.NON_REPLICATED_DEDUPLICATION_WINDOW)
                    .append(" = ")
                    .append(query.getDeduplicationWindow());
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

    private static class KeyWord {
        public static final String PARTITION_BY = "PARTITION BY";
        private static final String ENGINE = "ENGINE";
        private static final String TUPLE = "tuple";
        private static final String PARTITION = "PARTITION";
        private static final String FREEZE = "FREEZE";
        private static final String ATTACH_PART = "ATTACH PART";
        private static final String SETTINGS = "SETTINGS";
        private static final String NON_REPLICATED_DEDUPLICATION_WINDOW = "non_replicated_deduplication_window";

        private KeyWord() {
        }
    }
}
