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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CreateTable<T extends CreateTable<T> > extends DDL<T> {

    private static class Default extends CreateTable<Default> {
        public Default(TableIdentifier table, boolean ifNotExists) {
            super(table, ifNotExists);
        }
    }

    private final boolean ifNotExists;
    private final TableIdentifier table;
    private final List<ColumnWithType> columns;


    public CreateTable(TableIdentifier table, boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
        this.table = table;
        this.columns = new ArrayList<>();
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public TableIdentifier table() {
        return table;
    }

    public List<ColumnWithType> getColumns() {
        return columns;
    }

    @SuppressWarnings("unchecked")
    public T columns(Collection<? extends ColumnWithType> fields) {
        columns.addAll(fields);
        return (T) this;
    }
    public final T columns(ColumnWithType... fields) {
        return columns(Arrays.asList(fields));
    }

    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    public static CreateTable<Default> create(String database, String table) {
        return new Default(TableIdentifier.table(database, table), true);
    }
}
