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
package io.kyligence.kap.secondstorage.ddl.visitor;


import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.DropDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.ExistsDatabase;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.RenameTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.ShowCreateDatabase;
import io.kyligence.kap.secondstorage.ddl.ShowCreateTable;
import io.kyligence.kap.secondstorage.ddl.ShowDatabases;
import io.kyligence.kap.secondstorage.ddl.ShowTables;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;

public interface RenderVisitor {
    void visit(ColumnWithType column);
    void visit(TableIdentifier tableIdentifier);
    void visit(RenameTable renameTable);
    void visit(CreateTable<?> createTable);
    void visit(CreateDatabase createDatabase);
    void visit(ShowCreateDatabase showCreateDatabase);
    void visit(DropTable dropTable);
    void visit(DropDatabase dropDatabase);
    void visit(InsertInto insert);
    void visit(Select insert);
    void visit(GroupBy groupBy);
    void visit(AlterTable alterTable);
    void visit(ShowCreateTable showCreateTable);
    void visit(ExistsDatabase existsDatabase);
    void visit(ExistsTable existsTable);
    void visit(ShowDatabases showDatabases);
    void visit(ShowTables showTables);

    void visitValue(Object pram);

    void visit(AlterTable.ManipulatePartition movePartition);
}
