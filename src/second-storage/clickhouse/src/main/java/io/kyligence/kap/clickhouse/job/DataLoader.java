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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.secondstorage.ColumnMapping;
import io.kyligence.kap.secondstorage.SecondStorageConcurrentTestUtil;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.datatype.DataType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class DataLoader {

    static String clickHouseType(DataType dt) {
        switch (dt.getName()) {
        case DataType.BOOLEAN:
            return "UInt8";
        case DataType.BYTE:
        case DataType.TINY_INT:
            return "Int8";
        case DataType.SHORT:
        case DataType.SMALL_INT:
            return "Int16";
        case DataType.INT:
        case DataType.INT4:
        case DataType.INTEGER:
            return "Int32";
        case DataType.LONG:
        case DataType.LONG8:
        case DataType.BIGINT:
            return "Int64";
        case DataType.FLOAT:
            return "Float32";
        case DataType.DOUBLE:
            return "Float64";
        case DataType.DECIMAL:
        case DataType.NUMERIC:
            return String.format(Locale.ROOT, "Decimal(%d,%d)", dt.getPrecision(), dt.getScale());
        case DataType.VARCHAR:
        case DataType.CHAR:
        case DataType.STRING:
            return "String";
        case DataType.DATE:
            return "Date";
        case DataType.TIMESTAMP:
        case DataType.DATETIME:
            return "DateTime";
        case DataType.TIME:
        case DataType.REAL:
        case DataType.ANY_STR:
        case DataType.BINARY:
        case DataType.ARRAY:
            throw new UnsupportedOperationException("will support");
        default:
        }
        throw new UnsupportedOperationException("");
    }

    static List<ColumnWithType> columns(LayoutEntity layout, String partitionCol, boolean addPrefix) {
        List<ColumnWithType> cols = new ArrayList<>();
        layout.getOrderedDimensions()
                .forEach((k, v) -> cols.add(new ColumnWithType(
                        addPrefix ? getPrefixColumn(String.valueOf(k)) : String.valueOf(k), clickHouseType(v.getType()),
                        // partition column must not be null
                        v.getColumnDesc().isNullable() && !String.valueOf(k).equals(partitionCol), true)));
        return cols;
    }

    static String getPrefixColumn(String col) {
        return ColumnMapping.kapColumnToSecondStorageColumn(col);
    }

    private final String executableId;
    private final String database;
    private final Function<LayoutEntity, String> prefixTableName;
    private final Engine tableEngine;
    private final boolean isIncremental;

    public DataLoader(String executableId, String database, Function<LayoutEntity, String> prefixTableName, Engine tableEngine, boolean isIncremental) {
        this.executableId = executableId;
        this.database = database;
        this.prefixTableName = prefixTableName;
        this.tableEngine = tableEngine;
        this.isIncremental = isIncremental;
    }

    public boolean load(List<LoadInfo> loadInfoBatch, LoadContext loadContext) throws InterruptedException, ExecutionException, SQLException {
        val totalJdbcNum = loadInfoBatch.stream().mapToInt(item -> item.getNodeNames().length).sum();

        List<ShardLoader> shardLoaders = new ArrayList<>(totalJdbcNum + 2);
        // After new shard is created, JDBC Connection is Ready.
        for (val loadInfo : loadInfoBatch) {
            String[] nodeNames = loadInfo.getNodeNames();
            Preconditions.checkArgument(nodeNames.length == loadInfo.getShardFiles().size());

            for (int idx = 0; idx < nodeNames.length; idx++) {
                final String nodeName = nodeNames[idx];
                final List<String> listParquet = loadInfo.getShardFiles().get(idx);

                val builder = ShardLoader.ShardLoadContext.builder().jdbcURL(SecondStorageNodeHelper.resolve(nodeName))
                        .executableId(executableId)
                        .database(database).layout(loadInfo.getLayout()).parquetFiles(listParquet)
                        .tableEngine(tableEngine).destTableName(prefixTableName.apply(loadInfo.getLayout()));
                if (isIncremental) {
                    String partitionColName = loadInfo.model.getPartitionDesc().getPartitionDateColumn();
                    val dateCol = loadInfo.model.getAllNamedColumns().stream()
                            .filter(column -> column.getAliasDotColumn().equals(partitionColName)).findFirst()
                            .orElseThrow(
                                    () -> new IllegalStateException("can't find partition column " + partitionColName));
                    Preconditions.checkState(loadInfo.getLayout().getColumns().stream()
                            .anyMatch(col -> col.getAliasDotName().equals(dateCol.getAliasDotColumn())));
                    Preconditions.checkArgument(loadInfo.segment.getSegRange().getStart() instanceof Long);
                    builder.partitionFormat(loadInfo.model.getPartitionDesc().getPartitionDateFormat())
                            .partitionColumn(Objects.toString(dateCol.getId()))
                            .targetPartitions(SecondStorageDateUtils.splitByDay(loadInfo.segment.getSegRange()));
                }


                val needDropTable = getNeedDropTable(loadInfo);
                builder.needDropTable(needDropTable);
                builder.needDropPartition(getNeedDropPartition(loadInfo, needDropTable));

                ShardLoader.ShardLoadContext context = builder.build();
                loadInfo.setTargetDatabase(database);
                loadInfo.setTargetTable(prefixTableName.apply(loadInfo.getLayout()));
                shardLoaders.add(new ShardLoader(context));
            }
        }

        // skip segment when committed
        val segmentKey = new LoadContext.CompletedSegmentKeyUtil(loadInfoBatch.get(0).getLayout().getId());
        if (loadContext.getHistorySegments(segmentKey).contains(loadInfoBatch.get(0).getSegmentId())) return false;

        final ExecutorService executorService = new ThreadPoolExecutor(totalJdbcNum, totalJdbcNum, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new NamedThreadFactory("LoadWoker"));
        CountDownLatch latch = new CountDownLatch(totalJdbcNum);
        List<Future<?>> futureList = Lists.newArrayList();

        AtomicBoolean stopFlag = new AtomicBoolean();
        SecondStorageConcurrentTestUtil.wait(SecondStorageConcurrentTestUtil.WAIT_PAUSED);

        for (ShardLoader shardLoader : shardLoaders) {
            Future<?> future = executorService.submit(() -> {
                String replicaName = shardLoader.getClickHouse().getShardName();
                val fileKey = new LoadContext.CompletedFileKeyUtil(replicaName, shardLoader.getLayout().getId());
                try (SetThreadName ignored = new SetThreadName("Shard %s", replicaName)) {
                    log.info("Load parquet files into {}", replicaName);
                    shardLoader.setup(loadContext.isNewJob());
                    val files = shardLoader.loadDataIntoTempTable(
                            loadContext.getHistory(fileKey),
                            stopFlag);
                    files.forEach(file -> loadContext.finishSingleFile(fileKey, file));
                    // save progress
                    return true;
                } finally {
                    latch.countDown();
                }
            });
            futureList.add(future);
        }
        boolean paused;
        do {
            stopFlag.set(loadContext.getJobStatus() == ExecutableState.DISCARDED
                    || loadContext.getJobStatus() == ExecutableState.PAUSED);
            paused = loadContext.getJobStatus() == ExecutableState.PAUSED;
        } while (!latch.await(5, TimeUnit.SECONDS));

        try {
            for (Future<?> future : futureList) {
                future.get();
            }
            SecondStorageConcurrentTestUtil.wait(SecondStorageConcurrentTestUtil.WAIT_BEFORE_COMMIT);
            paused = loadContext.getJob().isPaused();
            stopFlag.set(loadContext.getJob().isDiscarded() || paused);
            if (!stopFlag.get()) {
                // commit data when job finish normally
                for (ShardLoader shardLoader : shardLoaders) {
                        shardLoader.commit();
                }
                loadContext.finishSegment(loadInfoBatch.get(0).getSegmentId(), segmentKey);
            }
            SecondStorageConcurrentTestUtil.wait(SecondStorageConcurrentTestUtil.WAIT_AFTER_COMMIT);
            return paused;
        } catch (SQLException e) {
            for (ShardLoader shardLoader : shardLoaders) {
                shardLoader.cleanIncrementLoad();
            }
            return ExceptionUtils.rethrow(e);
        } finally {
            for (ShardLoader shardLoader : shardLoaders) {
                // if paused skip clean insert temp table
                shardLoader.cleanUpQuietly(paused);
            }

            if (!executorService.isShutdown()) {
                executorService.shutdown();
            }
        }
    }


    private Set<String> getNeedDropTable(LoadInfo loadInfo) {
        return loadInfo.containsOldSegmentTableData.stream().filter(tableData -> {
            val all = tableData.getAllSegments();
            return tableData.getLayoutID() != loadInfo.getLayout().getId() && all.size() == 1 && all.contains(loadInfo.oldSegmentId);
        }).map(TableData::getTable).collect(Collectors.toSet());
    }

    private Set<String> getNeedDropPartition(LoadInfo loadInfo, Set<String> needDropTable) {
        return loadInfo.containsOldSegmentTableData.stream()
                .filter(tableData -> tableData.getLayoutID() != loadInfo.getLayout().getId() && !needDropTable.contains(tableData.getTable()))
                .map(TableData::getTable)
                .collect(Collectors.toSet());
    }

}
