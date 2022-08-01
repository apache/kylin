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

import io.kyligence.kap.clickhouse.ddl.ClickHouseCreateTable;
import io.kyligence.kap.clickhouse.ddl.ClickHouseRender;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClickHouseDDLTest {

    @Test
    public void testCKCreateTable(){
        final ClickHouseCreateTable create =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()");
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple()", create.toSql(render));

        final ClickHouseCreateTable create2 =
                ClickHouseCreateTable.createCKTable("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()");
        assertEquals("CREATE TABLE `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple()", create2.toSql(render));
    }

    @Test
    public void testCKCreateTableWithDeduplicationWindow(){
        final ClickHouseCreateTable create =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()")
                        .deduplicationWindow(3);
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 3", create.toSql(render));

        final ClickHouseCreateTable create2 =
                ClickHouseCreateTable.createCKTable("pufa", "xx")
                        .columns(new ColumnWithType("a", "int"))
                        .engine("MergeTree()")
                        .deduplicationWindow(3);
        assertEquals("CREATE TABLE `pufa`.`xx`(a int) ENGINE = MergeTree() ORDER BY tuple() SETTINGS non_replicated_deduplication_window = 3", create2.toSql(render));
    }

    @Test
    public void testCKCreateTableLike() {
        final ClickHouseCreateTable createLike =
                ClickHouseCreateTable.createCKTableIgnoreExist("pufa", "ut")
                        .likeTable("pufa", "xx")
                        .engine("HDFS(xxx)");
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("CREATE TABLE if not exists `pufa`.`ut` AS `pufa`.`xx` ENGINE = HDFS(xxx)", createLike.toSql(render));
    }

    @Test
    public void testCKMovePartition() {
        final AlterTable alterTable = new AlterTable(
                TableIdentifier.table("test", "table1"),
                new AlterTable.ManipulatePartition("2020-01-01",
                        TableIdentifier.table("test", "table2"), AlterTable.PartitionOperation.MOVE));
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("ALTER TABLE `test`.`table1` MOVE PARTITION '2020-01-01' TO TABLE `test`.`table2`",
                alterTable.toSql(render));
    }

    @Test
    public void testCKDropPartition() {
        final AlterTable alterTable = new AlterTable(
                TableIdentifier.table("test", "table1"),
                new AlterTable.ManipulatePartition("2020-01-01", AlterTable.PartitionOperation.DROP));
        final ClickHouseRender render = new ClickHouseRender();
        assertEquals("ALTER TABLE `test`.`table1` DROP PARTITION '2020-01-01'",
                alterTable.toSql(render));
    }

}
