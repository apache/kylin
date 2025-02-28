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

package org.apache.kylin.rec.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.query.relnode.OlapContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ModelTreeTest extends NLocalFileMetadataTestCase {

    @BeforeEach
    void setUp() {
        createTestMetadata();
    }

    @AfterEach
    void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    void testEquals() {
        TableDesc rootFactTable1 = TableDesc.mockup("table1");
        TableDesc rootFactTable2 = TableDesc.mockup("table2");
        TableDesc tableDesc = TableDesc.mockup("tableDesc");
        tableDesc.setColumns(new ColumnDesc[] {});

        TableRef tableRef = new TableRef(new NDataModel(), "TableAlias", tableDesc, false);
        TblColRef idCol = new TblColRef(tableRef, ColumnDesc.mockup(tableDesc, 0, "id", "int"));
        TblColRef nameCol = new TblColRef(tableRef, ColumnDesc.mockup(tableDesc, 1, "T_1_1000AFB4:name", "String"));

        SQLDigest sqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(idCol)), // filter
                Lists.newArrayList(idCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true); // limit

        OlapContext olapContext1 = Mockito.mock(OlapContext.class);

        String sqlWithHint = "select /*+ MODEL_PRIORITY(model1) */ * from table";
        Mockito.doReturn(sqlWithHint).when(olapContext1).getSql();
        Mockito.doReturn(sqlDigest).when(olapContext1).getSQLDigest();

        Collection<OlapContext> contexts = Collections.singletonList(olapContext1);
        TableRef tableRef1 = new TableRef(new NDataModel(), "TableAlias1", tableDesc, false);
        TableRef tableRef2 = new TableRef(new NDataModel(), "TableAlias2", tableDesc, false);

        JoinTableDesc joinTableDesc = new JoinTableDesc();

        Map<String, JoinTableDesc> joins = new HashMap<>();
        joins.put("join1", joinTableDesc);

        Map<TableRef, String> tableRefAliasMap1 = new HashMap<>();
        tableRefAliasMap1.put(tableRef1, "alias1");

        ModelTree modelTree1 = new ModelTree(rootFactTable1, contexts, joins, tableRefAliasMap1);
        ModelTree modelTree2 = new ModelTree(rootFactTable1, contexts, joins, tableRefAliasMap1);

        // Case 1: Same object reference
        assertEquals(modelTree1, modelTree1);

        // Case 2: Different object type
        assertNotEquals(modelTree1, new Object());

        // Case 3: All attributes are the same
        assertEquals(modelTree1, modelTree2);

        // Case 4: Different rootFactTable
        ModelTree modelTree3 = new ModelTree(rootFactTable2, contexts, joins, tableRefAliasMap1);
        assertNotEquals(modelTree1, modelTree3);

        // Case 5: Different olapContexts
        OlapContext olapContext2 = Mockito.mock(OlapContext.class);
        Mockito.doReturn(sqlWithHint).when(olapContext2).getSql();
        ModelTree modelTree4 = new ModelTree(rootFactTable1, Collections.singletonList(olapContext2), joins,
                tableRefAliasMap1);
        sqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(nameCol)), // filter
                Lists.newArrayList(nameCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true);
        Mockito.doReturn(sqlDigest).when(olapContext2).getSQLDigest();
        assertNotEquals(modelTree1, modelTree4);

        // Case 6: Different olapContexts size
        ModelTree modelTree5 = new ModelTree(rootFactTable1, Lists.newArrayList(olapContext1, olapContext2), joins,
                tableRefAliasMap1);
        assertNotEquals(modelTree1, modelTree5);

        // Case 7: Different joins
        Map<String, JoinTableDesc> joins2 = new HashMap<>();
        joins2.put("join2", new JoinTableDesc());
        ModelTree modelTree6 = new ModelTree(rootFactTable1, contexts, joins2, tableRefAliasMap1);
        assertNotEquals(modelTree1, modelTree6);

        // Case 8: Different tableRefAliasMap
        Map<TableRef, String> tableRefAliasMap2 = new HashMap<>();
        tableRefAliasMap2.put(tableRef1, "alias1");
        tableRefAliasMap2.put(tableRef2, "alias2");
        ModelTree modelTree7 = new ModelTree(rootFactTable1, contexts, joins, tableRefAliasMap2);
        assertNotEquals(modelTree1, modelTree7);

        tableRefAliasMap2.remove(tableRef1);
        ModelTree modelTree8 = new ModelTree(rootFactTable1, contexts, joins, tableRefAliasMap2);
        assertNotEquals(modelTree1, modelTree8);
    }
}
