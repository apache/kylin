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

package io.kyligence.kap.secondstorage.ddl;

import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;
import io.kyligence.kap.secondstorage.ddl.visitor.Renderable;

public class AlterTable extends DDL<AlterTable> {
    private final TableIdentifier table;
    private ManipulatePartition manipulatePartition = null;
    private ManipulateIndex manipulateIndex = null;
    private boolean freeze = false;
    private String attachPart = null;

    private ModifyColumn modifyColumn = null;


    public AlterTable(TableIdentifier table, ManipulatePartition manipulatePartition) {
        this.table = table;
        this.manipulatePartition = manipulatePartition;
    }

    public AlterTable(TableIdentifier table, boolean freeze) {
        this.table = table;
        this.freeze = freeze;
    }

    public AlterTable(TableIdentifier table, String attachPart) {
        this.table = table;
        this.attachPart = attachPart;
    }

    public AlterTable(TableIdentifier table, ManipulateIndex manipulateIndex) {
        this.table = table;
        this.manipulateIndex = manipulateIndex;
    }

    public AlterTable(TableIdentifier table, ModifyColumn modifyColumn) {
        this.table = table;
        this.modifyColumn = modifyColumn;
    }

    public TableIdentifier getTable() {
        return table;
    }

    public ManipulatePartition getManipulatePartition() {
        return manipulatePartition;
    }

    public ManipulateIndex getManipulateIndex() {
        return manipulateIndex;
    }

    public ModifyColumn getModifyColumn() {
        return modifyColumn;
    }

    public boolean isFreeze() {
        return freeze;
    }

    public String getAttachPart() {
        return attachPart;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    public enum PartitionOperation {
        MOVE {
            @Override
            public String getOperation() {
                return "MOVE";
            }
        },
        DROP {
            @Override
            public String getOperation() {
                return "DROP";
            }
        };

        public abstract String getOperation();
    }

    public enum IndexOperation {
        ADD, MATERIALIZE, DROP;
    }

    public static class ManipulatePartition implements Renderable {
        private final String partition;
        private final TableIdentifier destTable;
        private final PartitionOperation partitionOperation;

        public ManipulatePartition(String partition, TableIdentifier destTable, PartitionOperation partitionOperation) {
            this.partition = partition;
            this.destTable = destTable;
            this.partitionOperation = partitionOperation;
        }

        public ManipulatePartition(String partition, PartitionOperation partitionOperation) {
            this.partition = partition;
            this.destTable = null;
            this.partitionOperation = partitionOperation;
        }

        public String getPartition() {
            return partition;
        }

        public TableIdentifier getDestTable() {
            return destTable;
        }

        public PartitionOperation getPartitionOperation() {
            return partitionOperation;
        }

        @Override
        public void accept(RenderVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class ManipulateIndex implements Renderable {
        private final String name;
        private String column;
        private String expr;
        private int granularity;
        private final IndexOperation indexOperation;

        public ManipulateIndex(String name, String column, String expr, int granularity) {
            this.name = name;
            this.column = column;
            this.expr = expr;
            this.granularity = granularity;
            this.indexOperation = IndexOperation.ADD;
        }

        public ManipulateIndex(String name, IndexOperation indexOperation) {
            this.name = name;
            this.indexOperation = indexOperation;
        }

        public IndexOperation getIndexOperation() {
            return indexOperation;
        }

        public int getGranularity() {
            return granularity;
        }

        public String getExpr() {
            return expr;
        }

        public String getColumn() {
            return this.column;
        }

        public String getName() {
            return name;
        }

        @Override
        public void accept(RenderVisitor visitor) {
            visitor.visit(this);
        }
    }

    public static class ModifyColumn implements Renderable {
        private final String column;

        private final String datatype;

        public ModifyColumn(String column, String datatype) {
            this.column = column;
            this.datatype = datatype;
        }

        public String getColumn() {
            return column;
        }

        public String getDatatype() {
            return datatype;
        }
        @Override
        public void accept(RenderVisitor visitor) {
            visitor.visit(this);
        }
    }
}
