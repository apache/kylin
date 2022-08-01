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
package org.apache.kylin.secondstorage.ddl.visitor;


import org.apache.kylin.secondstorage.ddl.AlterTable;
import org.apache.kylin.secondstorage.ddl.CreateDatabase;
import org.apache.kylin.secondstorage.ddl.CreateTable;
import org.apache.kylin.secondstorage.ddl.DropDatabase;
import org.apache.kylin.secondstorage.ddl.DropTable;
import org.apache.kylin.secondstorage.ddl.ExistsDatabase;
import org.apache.kylin.secondstorage.ddl.ExistsTable;
import org.apache.kylin.secondstorage.ddl.InsertInto;
import org.apache.kylin.secondstorage.ddl.RenameTable;
import org.apache.kylin.secondstorage.ddl.Select;
import org.apache.kylin.secondstorage.ddl.ShowCreateDatabase;
import org.apache.kylin.secondstorage.ddl.ShowCreateTable;
import org.apache.kylin.secondstorage.ddl.ShowDatabases;
import org.apache.kylin.secondstorage.ddl.ShowTables;
import org.apache.kylin.secondstorage.ddl.exp.ColumnWithType;
import org.apache.kylin.secondstorage.ddl.exp.GroupBy;
import org.apache.kylin.secondstorage.ddl.exp.TableIdentifier;

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
