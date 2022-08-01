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

import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithAlias;
import io.kyligence.kap.secondstorage.ddl.exp.ColumnWithType;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class DDLTest {
    @Test
    public void testCreateDb() {
        final CreateDatabase createDb = CreateDatabase.createDatabase("xx");
        assertEquals("CREATE DATABASE if not exists xx", createDb.toSql());
    }

    @Test
    public void testDropTable(){
        final DropTable drop = DropTable.dropTable("pufa", "xx");
        assertEquals("DROP TABLE if exists `pufa`.`xx`", drop.toSql());
    }

    @Test
    public void testRenameTable(){
        final RenameTable drop = RenameTable.renameSource("pufa", "xx_temp").to("pufa", "xx");
        assertEquals("RENAME TABLE `pufa`.`xx_temp` TO `pufa`.`xx`", drop.toSql());
    }

    @Test
    public void testCreateTable(){
        final CreateTable<?> create =
                CreateTable.create("pufa", "xx")
                .columns(new ColumnWithType("a", "int"));
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a int)", create.toSql());

        // test create table with nullable
        final CreateTable<?> create2 =
                CreateTable.create("pufa", "xx")
                        .columns(new ColumnWithType("a", "int", true));
        assertEquals("CREATE TABLE if not exists `pufa`.`xx`(a Nullable(int))", create2.toSql());
    }

    @Test
    public void testInsertInto() throws SQLException {
        final InsertInto insertInto = InsertInto.insertInto("pufa", "xx").from("pufa", "ut");
        assertEquals("INSERT INTO `pufa`.`xx` SELECT * FROM `pufa`.`ut`", insertInto.toSql());
    }

    @Test
    public void testInsertIntoValue() throws SQLException {
        final InsertInto insertInto =
                InsertInto.insertInto("pufa", "xx").set("column1", 42).set(
                        "column2", "xyz");
        assertEquals(
                "INSERT INTO `pufa`.`xx` (column1, column2) VALUES (42, 'xyz')",
                insertInto.toSql());
    }

    @Test
    public void testSelectDistinctWithAlias() {
        final Select select = new Select(TableIdentifier.table("test", "table"))
                .column(ColumnWithAlias.builder().distinct(true).name("col1").alias("value1").build())
                .column(ColumnWithAlias.builder().distinct(false).name("col2").build());
        assertEquals("SELECT DISTINCT `col1` AS value1, `col2` FROM `test`.`table`", select.toSql());
    }
}
