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

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.DropTable;
import io.kyligence.kap.secondstorage.ddl.InsertInto;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickhouseLoadFileLoad implements ClickhouseLoadActionUnit {
    private final ClickHouseRender render = new ClickHouseRender();
    private final String sourceTable;
    private final LoadContext.CompletedFileKeyUtil fileKey;
    @Getter
    private final String parquetFile;
    private final ShardLoader shardLoader;
    private final LoadContext loadContext;


    public ClickhouseLoadFileLoad(ShardLoader shardLoader, String sourceTable, String parquetFile) {
        this.shardLoader = shardLoader;
        this.loadContext = shardLoader.getLoadContext();
        this.sourceTable = sourceTable;
        this.parquetFile = parquetFile;
        this.fileKey = new LoadContext.CompletedFileKeyUtil(shardLoader.getClickHouse().getShardName(),
                shardLoader.getLayout().getId());
    }

    public void loadSingleFileToTemp(ClickHouse clickHouse) throws SQLException {
        dropTable(sourceTable, clickHouse);
        try {
            final ClickHouseCreateTable likeTable = ClickHouseCreateTable
                    .createCKTable(shardLoader.getDatabase(), sourceTable)
                    .likeTable(shardLoader.getDatabase(), shardLoader.getLikeTempTableName())
                    .engine(shardLoader.getTableEngine().apply(parquetFile));
            clickHouse.apply(likeTable.toSql(render));

            insertDataWithRetry(shardLoader.getInsertTempTableName(), sourceTable, clickHouse);
        } finally {
            dropTable(sourceTable, clickHouse);
        }
    }

    private void dropTable(String table, ClickHouse clickHouse) throws SQLException {
        final String dropSQL = DropTable.dropTable(shardLoader.getDatabase(), table).toSql(render);
        clickHouse.apply(dropSQL);
    }

    private void insertDataWithRetry(String destTable, String srcTable, ClickHouse clickHouse) throws SQLException {
        int interval = KylinConfig.getInstanceFromEnv().getSecondStorageLoadRetryInterval();
        int maxRetry = KylinConfig.getInstanceFromEnv().getSecondStorageLoadRetry();

        int retry = 0;
        SQLException exception = null;
        do {
            if (retry > 0) {
                pauseOnRetry(retry, interval);
                log.info("Retrying for the {}th time ", retry);
            }

            try {
                final InsertInto insertInto = InsertInto.insertInto(shardLoader.getDatabase(), destTable).from(shardLoader.getDatabase(), srcTable);
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
    private void pauseOnRetry(int retry, int interval) {
        long time = retry + 1L;
        log.info("Pause {} milliseconds before retry", time * interval);

        try {
            TimeUnit.MILLISECONDS.sleep(time * interval);
        } catch (InterruptedException e) {
            log.error("Load tiered storage file retry time sleep error", e);
            Thread.currentThread().interrupt();
        }
    }

    private boolean needRetry(int retry, int maxRetry, Exception e) {
        if (e == null || retry > maxRetry)
            return false;

        String msg = e.getMessage();
        return (StringUtils.containsIgnoreCase(msg, "broken pipe")
                || StringUtils.containsIgnoreCase(msg, "connection reset"))
                && StringUtils.containsIgnoreCase(msg, "HTTPSession");
    }

    @Override
    public void doAction(ClickHouse clickHouse) throws SQLException {
        if (loadContext.getHistory(fileKey).contains(parquetFile)) {
            return;
        }
        loadSingleFileToTemp(clickHouse);
        loadContext.finishSingleFile(fileKey, parquetFile);
    }
}
