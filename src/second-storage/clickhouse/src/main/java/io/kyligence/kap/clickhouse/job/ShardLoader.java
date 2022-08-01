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

import io.kyligence.kap.clickhouse.database.ClickHouseOperator;
import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.CreateDatabase;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import io.kyligence.kap.secondstorage.ddl.RenameTable;
import io.kyligence.kap.secondstorage.ddl.Select;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import java.sql.Date;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.kyligence.kap.clickhouse.job.DataLoader.columns;
import static io.kyligence.kap.clickhouse.job.DataLoader.getPrefixColumn;

@Getter
@Slf4j
public class ShardLoader {
    private final ClickHouse clickHouse;
    private final String database;
    private final ClickHouseRender render = new ClickHouseRender();
    private final Engine tableEngine;
    private final LayoutEntity layout;
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

    public ShardLoader(ShardLoadContext context) throws SQLException {
        this.clickHouse = new ClickHouse(context.jdbcURL);
        this.database = context.database;
        this.tableEngine = context.tableEngine;
        this.layout = context.layout;
        this.parquetFiles = context.parquetFiles;
        this.partitionColumn = context.partitionColumn;
        this.partitionFormat = context.partitionFormat;
        this.incremental = partitionColumn != null;
        this.destTableName = context.destTableName;
        this.insertTempTableName = context.executableId + "@" + destTableName + "_temp";
        this.destTempTableName = context.executableId + "@" + destTableName + "_" + NameUtil.TEMP_TABLE_FLAG + "_tmp";
        this.likeTempTableName = context.executableId + "@" + destTableName + "_" + NameUtil.TEMP_TABLE_FLAG + "_ke_like";
        this.targetPartitions = context.targetPartitions;
        this.needDropPartition = context.needDropPartition;
        this.needDropTable = context.needDropTable;
    }

    private void commitIncrementalLoad() throws SQLException {
        final ClickHouseCreateTable likeTable = ClickHouseCreateTable.createCKTableIgnoreExist(database, destTableName)
                .likeTable(database, insertTempTableName);
        clickHouse.apply(likeTable.toSql(render));
        Select queryPartition = new Select(TableIdentifier.table(database, insertTempTableName))
                .column(ColumnWithAlias.builder().name(getPrefixColumn(partitionColumn)).distinct(true).build());
        List<Date> partitions = clickHouse.queryPartition(queryPartition.toSql(render), partitionFormat);
        // clean exists partition data
        batchDropPartition(targetPartitions);
        batchMovePartition(partitions, committedPartition);
    }

    public void setup(boolean newJob) throws SQLException {
        //1. prepare database and temp table
        final CreateDatabase createDb = CreateDatabase.createDatabase(database);
        clickHouse.apply(createDb.toSql(render));
        if (newJob) {
            createTable(insertTempTableName, layout, partitionColumn, true);
        } else {
            createTableIfNotExist(insertTempTableName, layout, partitionColumn, true);
        }
        createTable(likeTempTableName, layout, partitionColumn, false);
    }

    public List<String> loadDataIntoTempTable(List<String> history, AtomicBoolean stopped) throws Exception {
        // 2 insert into temp
        List<String> completeFiles = new ArrayList<>();
        for (int index = 0; index < parquetFiles.size(); index++) {
            if (stopped.get()) break;
            if (history.contains(parquetFiles.get(index))) continue;
            loadOneFile(insertTempTableName, parquetFiles.get(index),
                    String.format(Locale.ROOT, "%s_src_%05d", insertTempTableName, index));
            completeFiles.add(parquetFiles.get(index));
        }
        return completeFiles;
    }

    private boolean tableNotExistError(SQLException e) {
        return e.getErrorCode() == ClickHouseErrorCode.UNKNOWN_TABLE;
    }

    public void commit() throws SQLException {
        if (isIncremental()) {
            commitIncrementalLoad();
        } else {
            commitFullLoad();
        }
    }

    private void commitFullLoad() throws SQLException {
        //3 rename with atomically
        final RenameTable renameToTempTemp = RenameTable.renameSource(database, destTableName).to(database,
                destTempTableName);
        ClickHouseOperator operator = new ClickHouseOperator(clickHouse);
        if (operator.listTables(database).contains(destTableName)) {
            clickHouse.apply(renameToTempTemp.toSql(render));
        }
        final RenameTable renameToDest = RenameTable.renameSource(database, insertTempTableName).to(database,
                destTableName);
        clickHouse.apply(renameToDest.toSql(render));
    }

    @Builder
    public static class ShardLoadContext {
        String executableId;
        String jdbcURL;
        String database;
        LayoutEntity layout;
        List<String> parquetFiles;
        String destTableName;
        Engine tableEngine;
        String partitionColumn;
        String partitionFormat;
        List<Date> targetPartitions;
        Set<String> needDropPartition;
        Set<String> needDropTable;
    }

    public void cleanIncrementLoad() throws SQLException {
        if (isIncremental()) {
            batchDropPartition(committedPartition);
        }
    }

    private void batchDropPartition(List<Date> partitions) throws SQLException {
        AlterTable alterTable;
        val dateFormat = new SimpleDateFormat(partitionFormat, Locale.getDefault(Locale.Category.FORMAT));
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, destTableName),
                    new AlterTable.ManipulatePartition(dateFormat.format(partition),
                            AlterTable.PartitionOperation.DROP));
            clickHouse.apply(alterTable.toSql(render));

            if (needDropPartition != null) {
                for (String table : needDropPartition) {
                    alterTable = new AlterTable(TableIdentifier.table(database, table),
                            new AlterTable.ManipulatePartition(dateFormat.format(partition),
                                    AlterTable.PartitionOperation.DROP));
                    clickHouse.apply(alterTable.toSql(render));
                }
            }
        }
    }

    private void batchMovePartition(List<Date> partitions, List<Date> successPartition) throws SQLException {
        AlterTable alterTable;
        val dateFormat = new SimpleDateFormat(partitionFormat, Locale.getDefault(Locale.Category.FORMAT));
        for (val partition : partitions) {
            alterTable = new AlterTable(TableIdentifier.table(database, insertTempTableName),
                    new AlterTable.ManipulatePartition(dateFormat.format(partition),
                            TableIdentifier.table(database, destTableName), AlterTable.PartitionOperation.MOVE));
            // clean partition data
            clickHouse.apply(alterTable.toSql(render));
            if (successPartition != null) {
                successPartition.add(partition);
            }
        }
    }

    public void cleanUp(boolean keepInsertTempTable) throws SQLException {
        if (!keepInsertTempTable) {
            dropTable(insertTempTableName);
        }
        dropTable(destTempTableName);
        dropTable(likeTempTableName);

        if (needDropTable != null) {
            for (String table : needDropTable) {
                dropTable(table);
            }
        }
    }

    public void cleanUpQuietly(boolean keepInsertTempTable) {
        try {
            this.cleanUp(keepInsertTempTable);
        } catch (SQLException e) {
            log.error("clean temp table on {} failed.", clickHouse.getPreprocessedUrl(), e);
        }
    }

    private void createTable(String table, LayoutEntity layout, String partitionBy, boolean addPrefix)
            throws SQLException {
        dropTable(table);

        final ClickHouseCreateTable mergeTable = ClickHouseCreateTable.createCKTable(database, table)
                .columns(columns(layout, partitionBy, addPrefix))
                .partitionBy(addPrefix && partitionBy != null ? getPrefixColumn(partitionBy) : partitionBy)
                .engine(Engine.DEFAULT)
                .deduplicationWindow(KylinConfig.getInstanceFromEnv().getSecondStorageLoadDeduplicationWindow());
        clickHouse.apply(mergeTable.toSql(render));
    }

    private void createTableIfNotExist(String table, LayoutEntity layout, String partitionBy, boolean addPrefix) throws SQLException {
        final ClickHouseCreateTable mergeTable = ClickHouseCreateTable.createCKTableIgnoreExist(database, table)
                .columns(columns(layout, partitionBy, addPrefix))
                .partitionBy(addPrefix && partitionBy != null ? getPrefixColumn(partitionBy) : partitionBy)
                .engine(Engine.DEFAULT)
                .deduplicationWindow(KylinConfig.getInstanceFromEnv().getSecondStorageLoadDeduplicationWindow());
        clickHouse.apply(mergeTable.toSql(render));
    }

    private void dropTable(String table) throws SQLException {
        final String dropSQL = DropTable.dropTable(database, table).toSql(render);
        clickHouse.apply(dropSQL);
    }

    private void loadOneFile(String destTable, String parquetFile, String srcTable) throws Exception {
        dropTable(srcTable);
        try {
             final ClickHouseCreateTable likeTable = ClickHouseCreateTable.createCKTable(database, srcTable)
                    .likeTable(database, likeTempTableName).engine(tableEngine.apply(parquetFile));
            clickHouse.apply(likeTable.toSql(render));

            insertDataWithRetry(destTable, srcTable);
        } finally {
            dropTable(srcTable);
        }
    }

    private void insertDataWithRetry(String destTable, String srcTable) throws Exception {
        int interval = KylinConfig.getInstanceFromEnv().getSecondStorageLoadRetryInterval();
        int maxRetry = KylinConfig.getInstanceFromEnv().getSecondStorageLoadRetry();

        int retry = 0;
        Exception exception = null;
        do {
            if (retry > 0) {
                pauseOnRetry(retry, interval);
                log.info("Retrying for the {}th time ", retry);
            }

            try {
                final InsertInto insertInto = InsertInto.insertInto(database, destTable).from(database, srcTable);
                clickHouse.apply(insertInto.toSql(render));
                exception = null;
            } catch (SQLException e) {
                exception = e;
                if (!needRetry(retry, maxRetry, exception))
                    throw exception;
            }

            retry++;
        } while (needRetry(retry, maxRetry, exception));
    }

    // pauseOnRetry should only works when retry has been triggered
    private void pauseOnRetry(int retry, int interval) throws InterruptedException {
        long time = retry + 1L;
        log.info("Pause {} milliseconds before retry", time * interval);
        TimeUnit.MILLISECONDS.sleep(time * interval);
    }

    private boolean needRetry(int retry, int maxRetry, Exception e) {
        if (e == null || retry > maxRetry)
            return false;

        String msg = e.getMessage();
        return (StringUtils.containsIgnoreCase(msg, "broken pipe")
                || StringUtils.containsIgnoreCase(msg, "connection reset"))
                && StringUtils.containsIgnoreCase(msg, "HTTPSession");
    }
}
