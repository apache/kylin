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

import static io.kyligence.kap.clickhouse.job.DataLoader.columns;
import static io.kyligence.kap.clickhouse.job.DataLoader.getPrefixColumn;
import static io.kyligence.kap.clickhouse.job.DataLoader.orderColumns;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;

import io.kyligence.kap.clickhouse.ClickHouseNameUtil;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.clickhouse.ddl.TableSetting;
import io.kyligence.kap.clickhouse.parser.DescQueryParser;
import io.kyligence.kap.clickhouse.parser.ExistsQueryParser;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.Desc;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.SkippingIndexChooser;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.metadata.TableEntity;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class ShardLoader {
    private final ClickHouse clickHouse;
    private final String database;
    private final ClickHouseRender render = new ClickHouseRender();
    private final Engine tableEngine;
    private final LayoutEntity layout;
    private final TableEntity tableEntity;
    private final String partitionColumn;
    private final String partitionFormat;
    private final List<String> parquetFiles;
    private final String destTableName;
    private final String insertTempTableName;
    private final String destTempTableName;
    private final String likeTempTableName;
    private final boolean incremental;
    private final List<Date> targetPartitions;
    private final List<Date> committedPartition = new ArrayList<>();
    private final Set<String> needDropPartition;
    private final Set<String> needDropTable;
    private final String jdbcURL;
    private final String nodeName;
    private final LoadContext loadContext;

    public ShardLoader(ShardLoadContext context) {
        this.clickHouse = new ClickHouse(context.jdbcURL);
        this.database = context.database;
        this.tableEngine = context.tableEngine;
        this.layout = context.layout;
        this.tableEntity = context.tableEntity;
        this.parquetFiles = context.parquetFiles;
        this.partitionColumn = context.partitionColumn;
        this.partitionFormat = context.partitionFormat;
        this.incremental = partitionColumn != null;
        this.destTableName = context.destTableName;
        this.insertTempTableName = ClickHouseNameUtil.getInsertTempTableName(context.executableId, context.segmentId,
                context.layout.getId());
        this.destTempTableName = ClickHouseNameUtil.getDestTempTableName(context.executableId, context.segmentId,
                context.layout.getId());
        this.likeTempTableName = ClickHouseNameUtil.getLikeTempTableName(context.executableId, context.segmentId,
                context.layout.getId());
        this.targetPartitions = context.targetPartitions;
        this.needDropPartition = context.needDropPartition;
        this.needDropTable = context.needDropTable;
        this.jdbcURL = context.jdbcURL;
        this.nodeName = context.nodeName;
        this.loadContext = context.loadContext;
    }

    public void createDestTableIgnoreExist() throws SQLException {
        final ClickHouseCreateTable likeTable = ClickHouseCreateTable.createCKTableIgnoreExist(database, destTableName)
                .likeTable(database, insertTempTableName);
        clickHouse.apply(likeTable.toSql(render));
    }

    public List<Date> getInsertTempTablePartition() throws SQLException {
        Select queryPartition = new Select(TableIdentifier.table(database, insertTempTableName))
                .column(ColumnWithAlias.builder().name(getPrefixColumn(partitionColumn)).distinct(true).build());
        return clickHouse.queryPartition(queryPartition.toSql(render), partitionFormat);
    }

    public void setup(boolean newJob) throws SQLException {
        //1. prepare database
        final CreateDatabase createDb = CreateDatabase.createDatabase(database);
        clickHouse.apply(createDb.toSql(render));
        //2. desc dest table
        int existCode = clickHouse.query(new ExistsTable(TableIdentifier.table(database, destTableName)).toSql(), ExistsQueryParser.EXISTS).get(0);
        Map<String, String> columnTypeMap = new HashMap<>();
        if (existCode == 1) {
            columnTypeMap = clickHouse.query(new Desc(TableIdentifier.table(database, destTableName)).toSql(render), DescQueryParser.Desc).stream()
                    .collect(Collectors.toMap(ClickHouseSystemQuery.DescTable::getColumn, ClickHouseSystemQuery.DescTable::getDatatype));
        }
        //3. prepare temp table
        if (newJob) {
            createTable(insertTempTableName, columnTypeMap, layout, partitionColumn, true);
        }
        createTable(likeTempTableName, columnTypeMap, layout, partitionColumn, false);
    }

    public List<ClickhouseLoadFileLoad> toSingleFileLoader() {
        List<ClickhouseLoadFileLoad> loaders = new ArrayList<>(parquetFiles.size());
        for (int index = 0; index < parquetFiles.size(); index++) {
            String sourceTable = ClickHouseNameUtil.getFileSourceTableName(insertTempTableName, index);
            loaders.add(new ClickhouseLoadFileLoad(this, sourceTable, parquetFiles.get(index)));
        }
        return loaders;
    }

    @Builder
    public static class ShardLoadContext {
        String executableId;
        String jdbcURL;
        String database;
        LayoutEntity layout;
        TableEntity tableEntity;
        List<String> parquetFiles;
        String destTableName;
        Engine tableEngine;
        String partitionColumn;
        String partitionFormat;
        List<Date> targetPartitions;
        Set<String> needDropPartition;
        Set<String> needDropTable;
        String segmentId;
        String nodeName;
        LoadContext loadContext;
    }

    public void cleanUp(boolean isPaused) throws SQLException {
        if (!isPaused) {
            dropTable(insertTempTableName);
        }
        dropTable(destTempTableName);
        dropTable(likeTempTableName);

        if (!isPaused && needDropTable != null) {
            for (String table : needDropTable) {
                dropTable(table);
            }
        }
    }

    public void cleanUpQuietly(boolean isPaused) {
        try {
            this.cleanUp(isPaused);
        } catch (SQLException e) {
            log.error("clean temp table on {} failed.", clickHouse.getPreprocessedUrl(), e);
        }
    }

    private void createTable(String table, Map<String, String> columnTypeMap, LayoutEntity layout, String partitionBy,
            boolean addPrefix) throws SQLException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel model = getLayout().getModel();
        NDataflow dataflow = NDataflowManager.getInstance(kylinConfig, model.getProject())
                .getDataflow(getLayout().getModel().getId());

        dropTable(table);
        final ClickHouseCreateTable mergeTable = ClickHouseCreateTable.createCKTable(database, table)
                .columns(columns(columnTypeMap, layout, partitionBy, addPrefix))
                .orderBy(orderColumns(layout, tableEntity.getPrimaryIndexColumns(), addPrefix))
                .partitionBy(addPrefix && partitionBy != null ? getPrefixColumn(partitionBy) : partitionBy)
                .engine(Engine.DEFAULT)
                .tableSettings(TableSetting.NON_REPLICATED_DEDUPLICATION_WINDOW,
                        String.valueOf(KylinConfig.getInstanceFromEnv().getSecondStorageLoadDeduplicationWindow()))
                .tableSettings(TableSetting.ALLOW_NULLABLE_KEY,
                        dataflow.getConfig().getSecondStorageIndexAllowNullableKey() ? "1" : "0");
        clickHouse.apply(mergeTable.toSql(render));
        if (addPrefix && CollectionUtils.isNotEmpty(tableEntity.getSecondaryIndexColumns())) {
            addSkippingIndex(table, layout, tableEntity.getSecondaryIndexColumns());
        }
    }

    private void addSkippingIndex(String table, LayoutEntity layoutEntity, Set<Integer> secondaryIndexColumns)
            throws SQLException {
        NDataModel model = layoutEntity.getModel();
        KylinConfig modelConfig = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                .getDataflow(model.getId()).getConfig();
        int granularity = modelConfig.getSecondStorageSkippingIndexGranularity();
        AlterTable alterTable;
        TableIdentifier tableIdentifier = TableIdentifier.table(database, table);

        for (Integer col : secondaryIndexColumns) {
            String columnName = getPrefixColumn(String.valueOf(col));
            String name = ClickHouseNameUtil.getSkippingIndexName(destTableName, columnName);
            String expr = SkippingIndexChooser
                    .getSkippingIndexType(layoutEntity.getOrderedDimensions().get(col).getType()).toSql(modelConfig);
            alterTable = new AlterTable(tableIdentifier,
                    new AlterTable.ManipulateIndex(name, columnName, expr, granularity));
            clickHouse.apply(alterTable.toSql(render));
        }
    }

    private void dropTable(String table) throws SQLException {
        final String dropSQL = DropTable.dropTable(database, table).toSql(render);
        clickHouse.apply(dropSQL);
    }
}
