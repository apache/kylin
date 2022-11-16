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

import static org.apache.commons.lang.StringUtils.join;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.map.ListOrderedMap;

import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.CreateTable;
import io.kyligence.kap.secondstorage.ddl.Desc;
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
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.GroupBy;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;

public class DefaultSQLRender implements BaseRender {

    protected static final String OPEN_BRACKET = "(";
    protected static final String CLOSE_BRACKET = ")";

    @Override
    public void visit(Select select) {
        result.append(KeyWord.SELECT);
        if (select.columns().isEmpty()) {
            result.append(" * ");
        } else {
            result.append(' ');
            List<String> columns = new ArrayList<>(select.columns().size());
            for (ColumnWithAlias column : select.columns()) {
                columns.add(buildColumnWithAlias(column));
            }
            result.append(String.join(",", columns));
            result.append(' ');
        }

        result.append(KeyWord.FROM);
        acceptOrVisitValue(select.from());
        if (select.where() != null) {
            result.append(' ').append(KeyWord.WHERE).append(' ').append(select.where());
        }
        if (select.groupby() != null) {
            result.append(' ');
            this.visit(select.groupby());
        }
    }

    private String buildColumnWithAlias(ColumnWithAlias column) {
        StringBuilder columnString = new StringBuilder();
        if (column.isDistinct()) {
            columnString.append(KeyWord.DISTINCT);
        }
        if (column.getExpr() != null) {
            columnString.append(' ').append(column.getExpr());
        } else {
            columnString.append(" `").append(column.getName()).append("`");
        }
        if (column.getAlias() != null) {
            columnString.append(" AS ").append(column.getAlias());
        }
        return columnString.toString();
    }

    @Override
    public void visit(GroupBy groupBy) {
        result.append(KeyWord.GROUP_BY);
        result.append(' ');
        List<String> columns = groupBy.columns().stream().map(col -> "`" + col.getName() + "`").collect(Collectors.toList());
        result.append(String.join(",", columns));
    }

    protected final StringBuilder result = new StringBuilder();

    // dialect
    protected String quoteColumn(String col) {
        return "`" + col + "`";
    }

    private String nullableColumn(String type) {
        return KeyWord.NULLABLE + OPEN_BRACKET + type + CLOSE_BRACKET;
    }

    public StringBuilder getResult() {
        return result;
    }

    public void reset() {
        result.setLength(0);
    }

    @Override
    public void visit(ColumnWithType column) {
        if (column.quote()) {
            result.append(quoteColumn(column.name()));
        } else {
            result.append(column.name());
        }
        String type = column.type();
        if (column.nullable()) {
            type = nullableColumn(type);
        }
        result.append(' ').append(type);
    }

    @Override
    public void visit(TableIdentifier tableIdentifier) {
        result.append(' ').append(tableIdentifier.table());
    }

    @Override
    public void visit(RenameTable renameTable) {
        result.append(KeyWord.RENAME).append(' ').append(KeyWord.TABLE);
        acceptOrVisitValue(renameTable.source());
        result.append(' ').append(KeyWord.TO);
        acceptOrVisitValue(renameTable.to());
    }

    protected void createTablePrefix(CreateTable<?> query) {
        result.append(KeyWord.CREATE).append(' ').append(KeyWord.TABLE);
        if (query.isIfNotExists()) {
            result.append(' ').append(KeyWord.IF_NOT_EXISTS);
        }
        acceptOrVisitValue(query.table());
    }

    @Override
    public void visit(CreateTable<?> query) {
        createTablePrefix(query);

        result.append('(');
        boolean firstColumn = true;
        for (final ColumnWithType column : query.getColumns()) {
            if (firstColumn)
                firstColumn = false;
            else
                result.append(",");
            acceptOrVisitValue(column);
        }
        result.append(')');
    }

    @Override
    public void visit(CreateDatabase createDatabase) {
        result.append(KeyWord.CREATE).append(' ').append(KeyWord.DATABASE);
        if (createDatabase.isIfNotExists()) {
            result.append(' ').append(KeyWord.IF_NOT_EXISTS);
        }
        result.append(' ').append(createDatabase.getDatabase());
    }

    @Override
    public void visit(DropTable dropTable) {
        result.append(KeyWord.DROP).append(' ').append(KeyWord.TABLE);
        if (dropTable.isIfExists()) {
            result.append(' ').append(KeyWord.IF_EXISTS);
        }
        acceptOrVisitValue(dropTable.table());
    }

    @Override
    public void visit(DropDatabase dropDatabase) {
        result.append(KeyWord.DROP).append(' ').append(KeyWord.DATABASE);
        if (dropDatabase.isIfExists()) {
            result.append(' ').append(KeyWord.IF_EXISTS);
        }
        result.append(' ').append(dropDatabase.db());
    }

    @Override
    public void visit(InsertInto insert) {
        result.append(KeyWord.INSERT);
        acceptOrVisitValue(insert.table());
        result.append(' ');
        if (insert.from() != null) {
            acceptOrVisitValue(insert.from());
        } else {
            result.append(OPEN_BRACKET);
            final ListOrderedMap columnValues = insert.getColumnsValues();
            result.append(join(columnValues.keyList(), ", "));
            result.append(CLOSE_BRACKET).append(" ");
            result.append(KeyWord.VALUES).append(" ").append(OPEN_BRACKET);
            boolean firstClause = true;
            for (final Object column : columnValues.keyList()) {
                if (firstClause) {
                    firstClause = false;
                } else {
                    result.append(", ");
                }
                acceptOrVisitValue(columnValues.get(column));
            }
            result.append(CLOSE_BRACKET);
        }
    }

    @Override
    public void visit(ShowCreateDatabase showCreateDatabase) {
        result.append(KeyWord.SHOW_CREATE_DATABASE).append(' ').append(showCreateDatabase.getDatabase());
    }

    @Override
    public void visit(ShowCreateTable showCreateTable) {
        result.append(KeyWord.SHOW_CREATE_TABLE).append(' ').append(showCreateTable.getTableIdentifier().table());
    }

    @Override
    public void visit(ExistsDatabase existsDatabase) {
        result.append(KeyWord.EXISTS).append(" ").append(KeyWord.DATABASE).append(" ").append(existsDatabase.getDatabase());
    }

    @Override
    public void visit(ExistsTable existsTable) {
        result.append(KeyWord.EXISTS).append(" ").append(KeyWord.TABLE).append(" ").append(existsTable.getTableIdentifier().table());
    }

    @Override
    public void visit(ShowDatabases showDatabases) {
        result.append("SHOW DATABASES");
    }

    @Override
    public void visit(ShowTables showTables) {
        result.append("SHOW TABLES").append(" FROM ").append(showTables.getDatabase());
    }

    @Override
    public void visit(AlterTable alterTable) {
        throw new UnsupportedOperationException("Alter table doesn't support by default render");
    }

    protected static class KeyWord {
        private KeyWord() {}
        public static final String CREATE = "CREATE";
        public static final String DATABASE = "DATABASE";
        public static final String SHOW_CREATE_DATABASE = "SHOW CREATE DATABASE";
        public static final String TABLE = "TABLE";
        public static final String DISTINCT = "DISTINCT";
        public static final String ALTER = "ALTER";
        public static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE";
        public static final String EXISTS = "EXISTS";
        public static final String WHERE = "WHERE";
        public static final String GROUP_BY = "GROUP BY";

        public static final String DROP = "DROP";
        public static final String IF_EXISTS = "if exists";
        public static final String IF_NOT_EXISTS = "if not exists";

        public static final String INSERT = "INSERT INTO";
        public static final String SELECT = "SELECT";
        public static final String FROM = "FROM";

        public static final String AS = "AS";
        public static final String ORDER_BY = "ORDER BY";

        public static final String RENAME = "RENAME";
        public static final String TO = "TO";

        private static final String VALUES = "VALUES";
        private static final String NULLABLE = "Nullable";

        private static final String DESC = "DESC";
    }

    @Override
    public void visit(AlterTable.ManipulatePartition movePartition) {
        throw new UnsupportedOperationException("Move Partition doesn't support by default render");
    }

    @Override
    public void visit(AlterTable.ManipulateIndex manipulateIndex) {
        throw new UnsupportedOperationException("ALTER INDEX doesn't support by default render");
    }

    @Override
    public void visit(AlterTable.ModifyColumn modifyColumn) {
        throw new UnsupportedOperationException("Modify Column doesn't support by default render");
    }

    @Override
    public void visit(Desc desc) {
        result.append(KeyWord.DESC)
                .append(' ')
                .append(desc.getTable().table());
    }

    @Override
    public void visitValue(Object pram) {
        if (pram instanceof String) {
            result.append("'").append(pram).append("'");
        } else {
            result.append(pram);
        }
    }
}
