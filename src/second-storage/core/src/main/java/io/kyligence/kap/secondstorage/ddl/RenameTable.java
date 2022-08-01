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

public class RenameTable extends DDL<RenameTable> {

    private final TableIdentifier source;
    private TableIdentifier to;

    private RenameTable(TableIdentifier source) {
        this.source = source;
    }

    public TableIdentifier source() {
        return source;
    }

    public RenameTable to(String database, String table) {
        this.to = TableIdentifier.table(database, table);
        return this;
    }

    public TableIdentifier to() {
        return to;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    public static RenameTable renameSource(String database, String table) {
        return new RenameTable(TableIdentifier.table(database, table));
    }
}
