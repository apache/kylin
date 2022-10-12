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

package io.kyligence.kap.clickhouse.job;

import static io.kyligence.kap.clickhouse.job.DataLoader.getPrefixColumn;

import java.sql.SQLException;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.clickhouse.ClickHouseNameUtil;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.parser.ExistsQueryParser;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.SkippingIndexChooser;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class RefreshSecondaryIndex {
    @JsonProperty("node")
    private String node;
    @JsonProperty("database")
    private String database;
    @JsonProperty("table")
    private String table;
    @JsonProperty("add_indexes")
    private Set<Integer> addIndexes;
    @JsonProperty("delete_indexes")
    private Set<Integer> deleteIndexes;

    @JsonIgnore
    private NDataflow dataflow;

    public RefreshSecondaryIndex() {
        // empty
    }

    public RefreshSecondaryIndex(String node, String database, String table, Set<Integer> addIndexes,
            Set<Integer> deleteIndexes, NDataflow dataflow) {
        this.node = node;
        this.database = database;
        this.table = table;
        this.dataflow = dataflow;
        this.addIndexes = addIndexes;
        this.deleteIndexes = deleteIndexes;
    }

    public void refresh() {
        TableIdentifier tableIdentifier = TableIdentifier.table(database, table);
        try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
            int existCode = clickHouse
                    .query(new ExistsTable(TableIdentifier.table(database, table)).toSql(), ExistsQueryParser.EXISTS)
                    .get(0);
            if (existCode != 1) {
                return;
            }
            Set<String> existSkipIndex = existSkippingIndex(clickHouse, database, table);

            for (Integer deleteIndexColumnId : deleteIndexes) {
                deleteSkippingIndex(clickHouse, tableIdentifier, deleteIndexColumnId, existSkipIndex);
            }

            for (Integer addIndexColumnId : addIndexes) {
                addSkippingIndex(clickHouse, tableIdentifier, addIndexColumnId, existSkipIndex);
            }
        } catch (SQLException e) {
            log.error("node {} update index {}.{} failed", node, database, table);
            ExceptionUtils.rethrow(e);
        }
    }

    private void addSkippingIndex(ClickHouse clickHouse, TableIdentifier tableIdentifier, int columnId,
            Set<String> existSkipIndex) throws SQLException {
        String column = getPrefixColumn(String.valueOf(columnId));
        String indexName = ClickHouseNameUtil.getSkippingIndexName(table, column);
        KylinConfig modelConfig = dataflow.getConfig();
        int granularity = modelConfig.getSecondStorageSkippingIndexGranularity();
        val render = new ClickHouseRender();

        String expr = SkippingIndexChooser
                .getSkippingIndexType(dataflow.getModel().getEffectiveDimensions().get(columnId).getType())
                .toSql(modelConfig);
        AlterTable alterTable = new AlterTable(tableIdentifier,
                new AlterTable.ManipulateIndex(indexName, column, expr, granularity));
        AlterTable materializeTable = new AlterTable(tableIdentifier,
                new AlterTable.ManipulateIndex(indexName, AlterTable.IndexOperation.MATERIALIZE));

        if (!existSkipIndex.contains(indexName)) {
            clickHouse.apply(alterTable.toSql(render));
        }
        clickHouse.apply(materializeTable.toSql(render));
    }

    private void deleteSkippingIndex(ClickHouse clickHouse, TableIdentifier tableIdentifier, int columnId,
            Set<String> existSkipIndex) throws SQLException {
        String indexName = ClickHouseNameUtil.getSkippingIndexName(table, getPrefixColumn(String.valueOf(columnId)));
        if (!existSkipIndex.contains(indexName)) {
            return;
        }
        AlterTable dropTable = new AlterTable(tableIdentifier,
                new AlterTable.ManipulateIndex(indexName, AlterTable.IndexOperation.DROP));
        clickHouse.apply(dropTable.toSql(new ClickHouseRender()));
    }

    public Set<String> existSkippingIndex(ClickHouse clickHouse, String database, String table) {
        try {
            return Sets.newHashSet(clickHouse.query(String.format(Locale.ROOT,
                    "select name from system.data_skipping_indices where database='%s' and table='%s'", database,
                    table), rs -> {
                        try {
                            return rs.getString("name");
                        } catch (SQLException e) {
                            return ExceptionUtils.rethrow(e);
                        }
                    }));
        } catch (Exception e) {
            log.warn("Query exist skipping error", e);
            ExceptionUtils.rethrow(e);
        }

        return Sets.newHashSet();
    }
}
