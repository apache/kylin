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
package org.apache.kylin.metadata.realization;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SQLDigestTest extends NLocalFileMetadataTestCase {

    @BeforeEach
    void setUp() {
        createTestMetadata();
    }

    @AfterEach
    void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    void testEqualsWithoutAlias() {
        TableDesc tableDesc = TableDesc.mockup("tableDesc");
        tableDesc.setColumns(new ColumnDesc[] {});
        TableRef tableRef1 = new TableRef(Mockito.mock(NDataModel.class), "T_1_1000AFB4", tableDesc, false);
        TblColRef idCol = new TblColRef(tableRef1, ColumnDesc.mockup(tableDesc, 0, "id", "int"));
        TblColRef nameCol = new TblColRef(tableRef1, ColumnDesc.mockup(tableDesc, 1, "name:", "String"));

        // same SQL digest
        SQLDigest sqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(idCol)), // filter
                Lists.newArrayList(idCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true); // limit
        SQLDigest othSqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(idCol)), // filter
                Lists.newArrayList(idCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true); // limit
        assertTrue(sqlDigest.equalsWithoutAlias(othSqlDigest));

        // equalsWithoutAlias when different SQL digest
        nameCol = new TblColRef(tableRef1, ColumnDesc.mockup(tableDesc, 1, "name", "String"));
        sqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(nameCol)), // filter
                Lists.newArrayList(idCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true); // limit
        TableRef tableRef2 = new TableRef(Mockito.mock(NDataModel.class), "T_2_9999AFB4", tableDesc, false);
        nameCol = new TblColRef(tableRef2, ColumnDesc.mockup(tableDesc, 1, "name", "String"));
        othSqlDigest = new SQLDigest("factTable", Sets.newHashSet(idCol, nameCol), Lists.newLinkedList(), // model
                Lists.newArrayList(), // groupByColumns
                Sets.newHashSet(), // subqueryJoinParticipants
                Sets.newHashSet(), // metricsColumns
                Lists.newArrayList(), // aggregation
                Sets.newLinkedHashSet(Collections.singleton(nameCol)), // filter
                Lists.newArrayList(idCol), Lists.newArrayList(SQLDigest.OrderEnum.ASCENDING), // sort
                10, true); // limit
        assertTrue(sqlDigest.equalsWithoutAlias(othSqlDigest));
    }
}