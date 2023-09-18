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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;

import java.util.ArrayList;
import java.util.List;

public class Select extends DDL<Select> {
    private final TableIdentifier fromTable;

    private final List<ColumnWithAlias> columns = new ArrayList<>();
    private String condition;
    private GroupBy groupby;

    public Select(TableIdentifier table) {
        fromTable = table;
    }

    public Select column(ColumnWithAlias column) {
        columns.add(column);
        return this;
    }

    public Select where(String condition) {
        this.condition = condition;
        return this;
    }

    public Select groupby(GroupBy groupby) {
        this.groupby = groupby;
        return this;
    }

    public void columns(List<ColumnWithAlias> columns) {
        this.columns.addAll(columns);
    }

    public List<ColumnWithAlias> columns() {
        return columns;
    }

    public TableIdentifier from() {
        return fromTable;
    }

    public String where() {
        return condition;
    }

    public GroupBy groupby() {
        return groupby;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }
}
