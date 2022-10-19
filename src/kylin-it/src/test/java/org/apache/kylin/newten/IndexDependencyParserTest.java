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

package org.apache.kylin.newten;

import java.util.Collection;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class IndexDependencyParserTest extends NLocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata("src/test/resources/ut_meta/heterogeneous_segment_2");
        overwriteSystemProp("kylin.query.engine.sparder-additional-files", "../../../kylin/build/conf/spark-executor-log4j.xml");
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void unwrapComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        var dataModelDesc = dataModelManager.getDataModelDesc("4a45dc4d-937e-43cc-8faa-34d59d4e11d3");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_NUM")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // 1+2
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(0, tblColRefList.size());

        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_LTAX")).findAny().get();

        // `LINEORDER`.`LO_TAX` +1
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> "LINEORDER.LO_TAX".equals(tblColRef.getIdentity())));

        dataModelDesc = dataModelManager.getDataModelDesc("0d146f1a-bdd3-4548-87ac-21c2c6f9a0da");

        computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_TOTAL_TAX")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // LINEORDER.LO_QUANTITY*LINEORDER.LO_TAX
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(2, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_QUANTITY")));
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_TAX")));

        computedColumn = dataModelDesc.getAllTableRefs().stream()
                .filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER")).map(TableRef::getColumns)
                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_EXTRACT")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // MINUTE(`LINEORDER`.`LO_ORDERDATE`)
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERDATE")));

        computedColumn = dataModelDesc.getAllTableRefs().stream()
                .filter(tableRef -> tableRef.getTableIdentity().equals("SSB.LINEORDER")).map(TableRef::getColumns)
                .flatMap(Collection::stream)
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_CAST_LO_ORDERKEY")).findAny().get();

        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        // cast(`lineorder`.`lo_orderkey` as double)
        tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());

        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> tblColRef.getIdentity().equals("LINEORDER.LO_ORDERKEY")));

    }

    @Test
    public void unwrapNestComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "cc_test");
        var dataModelDesc = dataModelManager.getDataModelDesc("4802b471-fb69-4b08-a45e-ab3e314e2f6c");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_LTAX_NEST")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(
                tblColRefList.stream().anyMatch(tblColRef -> "LINEORDER.LO_TAX".equals(tblColRef.getIdentity())));
    }

    @Test
    public void unwrapDateComputeColumn() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "heterogeneous_segment_2");
        var dataModelDesc = dataModelManager.getDataModelDesc("3f2860d5-0a4c-4f52-b27b-2627caafe769");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC2")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(1, tblColRefList.size());
        Assert.assertTrue(tblColRefList.stream()
                .anyMatch(tblColRef -> tblColRef.getExpressionInSourceDB().equals("KYLIN_SALES.PART_DT")));
    }

    @Test
    public void unwrapComputeColumnWithChineseAndSpecialChar() {
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                "special_character_in_column");
        var dataModelDesc = dataModelManager.getDataModelDesc("8c08822f-296a-b097-c910-e38d8934b6f9");
        var computedColumn = dataModelDesc.getEffectiveCols().values().stream()
                .filter(tblColRef -> tblColRef.getColumnDesc().getName().equals("CC_CHINESE_AND_SPECIAL_CHAR")).findAny().get();

        IndexDependencyParser parser = new IndexDependencyParser(dataModelDesc);

        Assert.assertNotNull(computedColumn);
        Assert.assertTrue(computedColumn.getColumnDesc().isComputedColumn());
        Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumn.getExpressionInSourceDB());
        Assert.assertEquals(3, tblColRefList.size());
    }
}
