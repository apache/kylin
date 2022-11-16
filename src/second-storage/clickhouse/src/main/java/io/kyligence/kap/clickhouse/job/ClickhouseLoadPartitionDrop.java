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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.AlterTable;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickhouseLoadPartitionDrop implements ClickhouseLoadActionUnit {
    private final ClickHouseRender render = new ClickHouseRender();
    private final List<String> needDropPartitionTable;
    private final Date partition;
    private final ShardLoader shardLoader;

    public ClickhouseLoadPartitionDrop(List<String> needDropPartitionTable, Date partition, ShardLoader shardLoader) {
        this.needDropPartitionTable = needDropPartitionTable;
        this.partition = partition;
        this.shardLoader = shardLoader;
    }

    public void dropPartition(ClickHouse clickHouse) throws SQLException {
        if (!shardLoader.isIncremental()) {
            return;
        }

        AlterTable alterTable;
        val dateFormat = new SimpleDateFormat(shardLoader.getPartitionFormat(),
                Locale.getDefault(Locale.Category.FORMAT));

        for (String table : needDropPartitionTable) {
            alterTable = new AlterTable(TableIdentifier.table(shardLoader.getDatabase(), table),
                    new AlterTable.ManipulatePartition(dateFormat.format(partition),
                            AlterTable.PartitionOperation.DROP));
            clickHouse.apply(alterTable.toSql(render));
        }
    }

    @Override
    public void doAction(ClickHouse clickHouse) throws SQLException {
        dropPartition(clickHouse);
    }
}
