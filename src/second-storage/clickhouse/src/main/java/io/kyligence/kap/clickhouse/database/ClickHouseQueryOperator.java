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

package io.kyligence.kap.clickhouse.database;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.kyligence.kap.secondstorage.ColumnMapping;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryMetrics;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseSystemQuery;
import io.kyligence.kap.clickhouse.parser.DescQueryParser;
import io.kyligence.kap.clickhouse.parser.ExistsQueryParser;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.database.QueryOperator;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.Desc;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseQueryOperator implements QueryOperator {

    private static final String NULLABLE_STRING = "Nullable(String)";

    private static final String LOW_CARDINALITY_STRING = "LowCardinality(Nullable(String))";

    private static final ClickHouseRender render = new ClickHouseRender();
    private final String project;

    public ClickHouseQueryOperator(String project) {
        this.project = project;
    }

    public Map<String, Object> getQueryMetric(String queryId) {
        if (!SecondStorageUtil.isProjectEnable(project) || StringUtils.isEmpty(queryId)) {
            return exceptionQueryMetric();
        }

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Set<String>> allShards = SecondStorageNodeHelper.groupsToShards(SecondStorageUtil.listNodeGroup(config, project));

        // filter if one node down
        List<Set<String>> upShards = allShards.stream()
                .map(replicas -> replicas.stream().filter(SecondStorageQueryRouteUtil::getNodeStatus).collect(Collectors.toSet()))
                .filter(replicas -> !replicas.isEmpty()).collect(Collectors.toList());

        if (upShards.size() != allShards.size()) {
            return exceptionQueryMetric();
        }

        String sql = ClickHouseSystemQuery.queryQueryMetric(queryId);
        Map<String, ClickHouseSystemQuery.QueryMetric> queryMetricMap = upShards.parallelStream().flatMap(replicas -> {
            Map<String, ClickHouseSystemQuery.QueryMetric> queryMetrics = new HashMap<>();
            try {
                queryMetrics.putAll(getQueryMetric(replicas, sql, false));

                if (!queryMetrics.isEmpty()) {
                    return queryMetrics.entrySet().stream();
                }

                queryMetrics.putAll(getQueryMetric(replicas, sql, true));
            } catch (SQLException ex) {
                log.error("Fetch tired storage query metric fail.", ex);
                queryMetrics.put(queryId,
                        ClickHouseSystemQuery.QueryMetric.builder().clientName(queryId).readBytes(-1).readRows(-1).resultRows(-1).build());
            }

            return queryMetrics.entrySet().stream();
        }).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (o1, o2) -> {
                    if (queryId.equals(o1.getClientName())) {
                        return o1;
                    }

                    if (queryId.equals(o2.getClientName())) {
                        return o2;
                    }

                    o1.setReadBytes(o2.getReadBytes() + o1.getReadBytes());
                    o1.setReadRows(o2.getReadRows() + o1.getReadRows());
                    o1.setResultRows(o2.getResultRows() + o1.getResultRows());
                    return o1;
                }
        ));

        if (queryMetricMap.containsKey(queryId)) {
            return exceptionQueryMetric();
        }

        Optional<String> lastQueryIdOptional = queryMetricMap.keySet().stream().filter(q -> !queryId.equals(q)).max(Comparator.comparing(String::toString));

        if (!lastQueryIdOptional.isPresent()) {
            return exceptionQueryMetric();
        }

        String lastQueryId = lastQueryIdOptional.get();
        return ImmutableMap.of(QueryMetrics.TOTAL_SCAN_COUNT, queryMetricMap.get(lastQueryId).getReadRows(), QueryMetrics.TOTAL_SCAN_BYTES, queryMetricMap.get(lastQueryId).getReadBytes(),
                QueryMetrics.SOURCE_RESULT_COUNT, queryMetricMap.get(lastQueryId).getResultRows());
    }

    public Map<String, ClickHouseSystemQuery.QueryMetric> getQueryMetric(Set<String> replicas, String sql, boolean needFlush) throws SQLException {
        Map<String, ClickHouseSystemQuery.QueryMetric> queryMetrics = new HashMap<>(3);
        for (String replica : replicas) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(replica))) {
                if (needFlush) {
                    clickHouse.apply("SYSTEM FLUSH LOGS");
                }

                List<ClickHouseSystemQuery.QueryMetric> result = clickHouse.query(sql, ClickHouseSystemQuery.QUERY_METRIC_MAPPER);

                for (ClickHouseSystemQuery.QueryMetric queryMetric : result) {
                    queryMetrics.put(queryMetric.getClientName(), queryMetric);
                }
            }
        }

        return queryMetrics;
    }

    private Map<String, Object> exceptionQueryMetric() {
        return ImmutableMap.of(QueryMetrics.TOTAL_SCAN_COUNT, -1L, QueryMetrics.TOTAL_SCAN_BYTES, -1L,
                QueryMetrics.SOURCE_RESULT_COUNT, -1L);
    }

    public void modifyColumnByCardinality(String database, String destTableName, Set<Integer> secondaryIndex) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<NodeGroup> nodeGroups = SecondStorageUtil.listNodeGroup(config, project);
        Set<String> nodes = nodeGroups.stream()
                .flatMap(x -> x.getNodeNames().stream())
                .filter(SecondStorageQueryRouteUtil::getNodeStatus)
                .collect(Collectors.toSet());
        String maxRowsNode = getMaxRowsNode(nodes, database, destTableName);
        if (maxRowsNode.isEmpty())
            return;

        ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(project);
        val tableColumns = getFilterDescTable(maxRowsNode, database, destTableName, projectInstance.getConfig());
        val modifyColumns = tableColumns.stream()
                .filter(col -> !secondaryIndex.contains(Integer.valueOf(ColumnMapping.secondStorageColumnToKapColumn(col.getColumn()))))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(modifyColumns))
            return;

        for (String node : nodes) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                modifyColumns.stream().forEach(c -> {
                    String targetType = NULLABLE_STRING.equals(c.getDatatype()) ? LOW_CARDINALITY_STRING : NULLABLE_STRING;
                    modifyColumn(clickHouse, database, destTableName, c.getColumn(), targetType);
                });
            } catch (Exception sqlException) {
                ExceptionUtils.rethrow(sqlException);
            }
        }
    }

    public void modifyColumnByCardinality(String database, String destTableName, String column, String datatype) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<NodeGroup> nodeGroups = SecondStorageUtil.listNodeGroup(config, project);
        Set<String> nodes = nodeGroups.stream()
                .flatMap(x -> x.getNodeNames().stream())
                .filter(SecondStorageQueryRouteUtil::getNodeStatus)
                .collect(Collectors.toSet());

        for (String node : nodes) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                modifyColumn(clickHouse, database, destTableName, column, datatype);
            } catch (Exception sqlException) {
                ExceptionUtils.rethrow(sqlException);
            }
        }
    }

    private String getMaxRowsNode(Set<String> nodes, String database, String destTableName) {
        String nodeName = "";
        long tableRows = 0L;
        for (String node : nodes) {
            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                int existCode = clickHouse.query(new ExistsTable(TableIdentifier.table(database, destTableName)).toSql(), ExistsQueryParser.EXISTS).get(0);
                if (existCode == 0)
                    continue;

                String select = new Select(TableIdentifier.table("system", "tables"))
                        .column(ColumnWithAlias.builder().expr("total_rows").alias("totalRows").build())
                        .where("database = '%s' AND name = '%s'").toSql();
                val rows = clickHouse.query(String.format(Locale.ROOT, select, database, destTableName), rs -> {
                    try {
                        return rs.getLong(1);
                    } catch (SQLException sqlException) {
                        return ExceptionUtils.rethrow(sqlException);
                    }
                }).get(0);

                if (rows > tableRows) {
                    tableRows = rows;
                    nodeName = node;
                }
            } catch (SQLException sqlException) {
                ExceptionUtils.rethrow(sqlException);
            }
        }
        return nodeName;
    }

    private List<ClickHouseSystemQuery.DescTable> getFilterDescTable(String node, String database, String destTableName, KylinConfig config) {
        try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
            val columnTypeMap = clickHouse.query(new Desc(TableIdentifier.table(database, destTableName)).toSql(), DescQueryParser.Desc)
                    .stream().filter(c -> NULLABLE_STRING.equals(c.getDatatype()) || LOW_CARDINALITY_STRING.equals(c.getDatatype()))
                    .collect(Collectors.toList());

            Map<String, Long> columnCardinalities = selectCardinality(clickHouse, database, destTableName, columnTypeMap).get(0);
            long lowCardinalityNumber = config.getSecondStorageLowCardinalityNumber();
            long highCardinalityNumber = config.getSecondStorageHighCardinalityNumber();
            return columnTypeMap.stream()
                    .filter(c -> (NULLABLE_STRING.equals(c.getDatatype()) && columnCardinalities.get(c.getColumn()) < lowCardinalityNumber)
                            || (LOW_CARDINALITY_STRING.equals(c.getDatatype()) && columnCardinalities.get(c.getColumn()) > highCardinalityNumber))
                    .collect(Collectors.toList());
        } catch (SQLException sqlException) {
            ExceptionUtils.rethrow(sqlException);
        }
        return Lists.newArrayList();
    }

    private List<Map<String, Long>> selectCardinality(ClickHouse clickHouse, String database, String table,
                                                      List<ClickHouseSystemQuery.DescTable> columnTypeMap) throws SQLException {
        final Select select = new Select(TableIdentifier.table(database, table));
        columnTypeMap.forEach(column ->
            select.column(ColumnWithAlias.builder().expr("uniqCombined(`" + column.getColumn() + "`)").alias(column.getColumn()).build())
        );
        return clickHouse.query(select.toSql(), rs -> {
            Map<String, Long> columnCardinality = Maps.newHashMap();
            for (int i = 0; i < columnTypeMap.size(); i++) {
                try {
                    columnCardinality.put(columnTypeMap.get(i).getColumn(), rs.getLong(i+1));
                } catch (SQLException e) {
                    return ExceptionUtils.rethrow(e);
                }
            }
            return columnCardinality;
        });
    }

    private void modifyColumn(ClickHouse clickHouse, String database, String table, String column, String datatype) {
        try {
            String alterTable = new AlterTable(TableIdentifier.table(database, table),
                    new AlterTable.ModifyColumn(column, datatype)).toSql(render);
            clickHouse.apply(alterTable);
        } catch (SQLException e) {
             ExceptionUtils.rethrow(e);
        }
    }
}
