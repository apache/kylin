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
import org.apache.commons.collections.map.ListOrderedMap;

import java.sql.SQLException;

public class InsertInto extends DDL<InsertInto> {

    private final TableIdentifier table;
    private Select select;
    private final ListOrderedMap columnsValues = new ListOrderedMap();

    public InsertInto(TableIdentifier table) {
        this.table = table;
        this.select = null;
    }

    public TableIdentifier table() {
        return table;
    }

    public InsertInto from(String database, String table) {
        select = new Select(TableIdentifier.table(database, table));
        return this;
    }

    public InsertInto set(final String column, final Object value) {
//        if (columnsValues.containsKey(column))
//            throw new QueryGrammarException("Column '" + column
//                    + "' has already been set.");
        columnsValues.put(column, value);
        return this;
    }

    public InsertInto set(final String column, final Object value,
                           final Object defaultValueIfNull) {
        if (value == null)
            return set(column, defaultValueIfNull);
        else
            return set(column, value);
    }

    public Select from() {
        return select;
    }

    public ListOrderedMap getColumnsValues() {
        return columnsValues;
    }

    @Override
    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }

    // throw SQLException for test
    public static InsertInto insertInto(String database, String table) throws SQLException {
        return new InsertInto(TableIdentifier.table(database, table));
    }
}
