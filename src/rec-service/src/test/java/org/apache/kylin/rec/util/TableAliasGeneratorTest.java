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

package org.apache.kylin.rec.util;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rec.common.AutoTestOnLearnKylinData;
import org.junit.Assert;
import org.junit.Test;

public class TableAliasGeneratorTest extends AutoTestOnLearnKylinData {

    @Test
    public void testJoinHierarchy() {
        String[] testTableNames = { "TABLEA", "TABLEB", "TABLEC", "TABLED" };
        TableAliasGenerator.TableAliasDict dict = TableAliasGenerator.generateNewDict(testTableNames);

        TableRef tableARef = mockTableRef("TABLEA", "COLA");
        TableRef tableBRef = mockTableRef("TABLEB", "COLB");
        TableRef tableCRef = mockTableRef("TABLEC", "COL");
        TableRef tableDRef = mockTableRef("TABLED", "COL");
        String joinHierarchy = dict.getHierarchyAliasFromJoins(new JoinDesc[] { //
                mockJoinDesc(tableBRef, tableARef, "COLB", "COLA"), //
                mockJoinDesc(tableCRef, tableBRef, "COL", "COLB"), //
                mockJoinDesc(tableDRef, tableBRef, "COL", "COLB") });
        Assert.assertEquals(
                "T0_KEY_[COLA]__TO__T1_KEY_[COLB]_KEY_[COLB]__TO__T2_KEY_[COL]_KEY_[COLB]__TO__T3_KEY_[COL]",
                joinHierarchy);
    }

    @Test
    public void testTableAliasGenerator() {
        String[] testTableNames = { "database1.table1", "database1.table3", "database1.table2", //
                "database2.table1", "database2.table4", "database2.table3", "database2.table2" };
        TableAliasGenerator.TableAliasDict dict = TableAliasGenerator.generateNewDict(testTableNames);
        Assert.assertEquals("D0_T0", dict.getAlias("database1.table1"));
        Assert.assertEquals("D0_T1", dict.getAlias("database1.table2"));
        Assert.assertEquals("database2.table4", dict.getTableName("D1_T3"));
        Assert.assertEquals("", dict.getHierarchyAliasFromJoins(null));
    }

    private TableRef mockTableRef(String tableName, String col) {
        TableDesc tableDesc = TableDesc.mockup(tableName);
        tableDesc.setColumns(new ColumnDesc[] { ColumnDesc.mockup(tableDesc, 0, col, "string") });
        return new TableRef(new NDataModel(), tableName, tableDesc, false);
    }

    private JoinDesc mockJoinDesc(TableRef pTable, TableRef fTable, String pk, String fk) {
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setPrimaryKey(new String[] { pk });
        joinDesc.setForeignKey(new String[] { fk });
        joinDesc.setPrimaryKeyColumns(new TblColRef[] { pTable.getColumn(pk) });
        joinDesc.setForeignKeyColumns(new TblColRef[] { fTable.getColumn(fk) });
        joinDesc.setPrimaryTableRef(pTable);
        joinDesc.setForeignTableRef(fTable);
        joinDesc.setType("LEFT");
        return joinDesc;
    }
}
