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

package org.apache.kylin.rest.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TableSchemaUpdateChecker {
    private final MetadataManager metadataManager;
    private final CubeManager cubeManager;

    static class CheckResult {
        private final boolean valid;
        private final String reason;

        private CheckResult(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }

        void raiseExceptionWhenInvalid() {
            if (!valid) {
                throw new RuntimeException(reason);
            }
        }

        static CheckResult validOnFirstLoad(String tableName) {
            return new CheckResult(true, format("Table '%s' hasn't been loaded before", tableName));
        }

        static CheckResult validOnCompatibleSchema(String tableName) {
            return new CheckResult(true, format("Table '%s' is compatible with all existing cubes", tableName));
        }

        static CheckResult invalidOnFetchSchema(String tableName, Exception e) {
            return new CheckResult(false, format("Failed to fetch metadata of '%s': %s", tableName, e.getMessage()));
        }

        static CheckResult invalidOnIncompatibleSchema(String tableName, List<String> reasons) {
            StringBuilder buf = new StringBuilder();
            for (String reason : reasons) {
                buf.append("- ").append(reason).append("\n");
            }

            return new CheckResult(false, format("Found %d issue(s) with '%s':%n%s Please disable and purge related cube(s) first", reasons.size(), tableName, buf.toString()));
        }
    }

    TableSchemaUpdateChecker(MetadataManager metadataManager, CubeManager cubeManager) {
        this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        this.cubeManager = checkNotNull(cubeManager, "cubeManager is null");
    }

    private List<CubeInstance> findCubeByTable(final TableDesc table) {
        Iterable<CubeInstance> relatedCubes = Iterables.filter(cubeManager.listAllCubes(), new Predicate<CubeInstance>() {
            @Override
            public boolean apply(@Nullable CubeInstance cube) {
                if (cube == null || cube.allowBrokenDescriptor()) {
                    return false;
                }
                DataModelDesc model = cube.getModel();
                if (model == null)
                    return false;
                return model.containsTable(table);
            }
        });

        return ImmutableList.copyOf(relatedCubes);
    }

    private boolean isColumnCompatible(ColumnDesc column, ColumnDesc newCol) {
        if (!column.getName().equalsIgnoreCase(newCol.getName())) {
            return false;
        }

        if (column.getType().isIntegerFamily()) {
            // OLAPTable.listSourceColumns converts some integer columns to bigint,
            // therefore strict type comparison won't work.
            // changing from one integer type to another should be fine.
            return newCol.getType().isIntegerFamily();
        } else if (column.getType().isNumberFamily()) {
            // Both are float/double should be fine.
            return newCol.getType().isNumberFamily();
        } else {
            // only compare base type name, changing precision or scale should be fine
            return column.getTypeName().equals(newCol.getTypeName());
        }
    }

    /**
     * check whether all columns used in `cube` has compatible schema in current hive schema denoted by `fieldsMap`.
     * @param cube cube to check, must use `table` in its model
     * @param origTable kylin's table metadata
     * @param fieldsMap current hive schema of `table`
     * @return true if all columns used in `cube` has compatible schema with `fieldsMap`, false otherwise
     */
    private List<String> checkAllColumnsInCube(CubeInstance cube, TableDesc origTable, TableDesc newTable) {
        Set<ColumnDesc> usedColumns = Sets.newHashSet();
        for (TblColRef col : cube.getAllColumns()) {
            usedColumns.add(col.getColumnDesc());
        }

        List<String> violateColumns = Lists.newArrayList();
        for (ColumnDesc column : origTable.getColumns()) {
            if (!column.isComputedColumnn() && usedColumns.contains(column)) {
                ColumnDesc newCol = newTable.findColumnByName(column.getName());
                if (newCol == null || !isColumnCompatible(column, newCol)) {
                    violateColumns.add(column.getName());
                }
            }
        }
        return violateColumns;
    }

    /**
     * check whether all columns in `table` are still in `fields` and have the same index as before.
     *
     * @param table kylin's table metadata
     * @param fields current table metadata in hive
     * @return true if only new columns are appended in hive, false otherwise
     */
    private boolean checkAllColumnsInTableDesc(TableDesc origTable, TableDesc newTable) {
        if (origTable.getColumnCount() > newTable.getColumnCount()) {
            return false;
        }

        ColumnDesc[] columns = origTable.getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (!isColumnCompatible(columns[i], newTable.getColumns()[i])) {
                return false;
            }
        }
        return true;
    }

    public CheckResult allowReload(TableDesc newTableDesc, String prj) {
        final String fullTableName = newTableDesc.getIdentity();

        TableDesc existing = metadataManager.getTableDesc(fullTableName, prj);
        if (existing == null) {
            return CheckResult.validOnFirstLoad(fullTableName);
        }

        List<String> issues = Lists.newArrayList();
        for (CubeInstance cube : findCubeByTable(newTableDesc)) {
            String modelName = cube.getModel().getName();

            // if user reloads a fact table used by cube, then all used columns must match current schema
            if (cube.getModel().isFactTable(fullTableName)) {
                TableDesc factTable = cube.getModel().findFirstTable(fullTableName).getTableDesc();
                List<String> violateColumns = checkAllColumnsInCube(cube, factTable, newTableDesc);
                if (!violateColumns.isEmpty()) {
                    issues.add(format("Column %s used in cube[%s] and model[%s], but changed in hive", violateColumns, cube.getName(), modelName));
                }
            }

            // if user reloads a lookup table used by cube, only append column(s) are allowed, all existing columns
            // must be the same (except compatible type changes)
            if (cube.getModel().isLookupTable(fullTableName)) {
                TableDesc lookupTable = cube.getModel().findFirstTable(fullTableName).getTableDesc();
                if (!checkAllColumnsInTableDesc(lookupTable, newTableDesc)) {
                    issues.add(format("Table '%s' is used as Lookup Table in cube[%s] and model[%s], but changed in hive", lookupTable.getIdentity(), cube.getName(), modelName));
                }
            }
        }

        if (issues.isEmpty()) {
            return CheckResult.validOnCompatibleSchema(fullTableName);
        }
        return CheckResult.invalidOnIncompatibleSchema(fullTableName, issues);
    }
}
