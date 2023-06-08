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

package org.apache.kylin.rest.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinDesc;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinTableDesc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

class ModelRequestTest {

    @Test
    void testModelRequestUpperCase() {
        ModelRequest modelRequest = new ModelRequest();
        testRootFactTableName(modelRequest);
        testSimplifiedDimensions(modelRequest);
        testJoinTables(modelRequest);
    }

    private void testRootFactTableName(ModelRequest modelRequest) {
        modelRequest.setRootFactTableName("test_TaBle");
        modelRequest.toUpperCaseModelRequest();
        Assertions.assertEquals("TEST_TABLE", modelRequest.getRootFactTableName());
    }

    private void testSimplifiedDimensions(ModelRequest modelRequest) {
        mockSimplifiedDimensions(modelRequest);
        modelRequest.toUpperCaseModelRequest();
        List<String> expected = Arrays.asList("SCHEMA.TEST_TABLE.COLUMN1", "SCHEMA.TEST_TABLE.COLUMN2",
                "SCHEMA.TEST_TABLE2.COLUMN");
        List<String> actual = modelRequest.getSimplifiedDimensions().stream()
                .map(NDataModel.NamedColumn::getAliasDotColumn).collect(Collectors.toList());
        Assertions.assertEquals(expected, actual);
    }

    private void testJoinTables(ModelRequest modelRequest) {
        mockJoinTables(modelRequest);
        modelRequest.toUpperCaseModelRequest();

        List<String> actualTables = modelRequest.getJoinTables().stream().map(JoinTableDesc::getTable)
                .collect(Collectors.toList());
        List<String> expectedTables = Arrays.asList("TEST_TABLE1", "TEST_TABLE2", "TEST_TABLE3", "TEST_TABLE4");
        Assertions.assertEquals(expectedTables, actualTables);

        List<String> actualForeignKeys = modelRequest.getJoinTables().stream().map(JoinTableDesc::getJoin)
                .map(JoinDesc::getForeignKey).filter(Objects::nonNull).map(Arrays::asList).flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<String> expectedForeignKeys = Arrays.asList("TEST_TABLE2.COLUMN1", "TEST_TABLE3.COLUMN2");
        Assertions.assertEquals(expectedForeignKeys, actualForeignKeys);

        List<String> actualPrimaryKeys = modelRequest.getJoinTables().stream().map(JoinTableDesc::getJoin)
                .map(JoinDesc::getPrimaryKey).filter(Objects::nonNull).map(Arrays::asList).flatMap(Collection::stream)
                .collect(Collectors.toList());
        List<String> expectedPrimaryKeys = Arrays.asList("TEST_TABLE1.COLUMN1", "TEST_TABLE1.COLUMN2",
                "TEST_TABLE3.COLUMN3");
        Assertions.assertEquals(expectedPrimaryKeys, actualPrimaryKeys);

        List<String> actualSimplifiedTables = modelRequest.getSimplifiedJoinTableDescs().stream()
                .map(JoinTableDesc::getTable).collect(Collectors.toList());
        List<String> expectedSimplifiedTables = Arrays.asList("TEST_TABLE1", "TEST_TABLE2", "TEST_TABLE3",
                "TEST_TABLE4");
        Assertions.assertEquals(expectedSimplifiedTables, actualSimplifiedTables);

        List<String> actualSimplifiedForeignKeys = modelRequest.getSimplifiedJoinTableDescs().stream()
                .map(SimplifiedJoinTableDesc::getSimplifiedJoinDesc).map(SimplifiedJoinDesc::getForeignKey)
                .filter(Objects::nonNull).map(Arrays::asList).flatMap(Collection::stream).collect(Collectors.toList());
        List<String> expectedSimplifiedForeignKeys = Arrays.asList("TEST_TABLE2.COLUMN1", "TEST_TABLE3.COLUMN2");
        Assertions.assertEquals(expectedSimplifiedForeignKeys, actualSimplifiedForeignKeys);

        List<String> actualSimplifiedPrimaryKeys = modelRequest.getSimplifiedJoinTableDescs().stream()
                .map(SimplifiedJoinTableDesc::getSimplifiedJoinDesc).map(SimplifiedJoinDesc::getPrimaryKey)
                .filter(Objects::nonNull).map(Arrays::asList).flatMap(Collection::stream).collect(Collectors.toList());
        List<String> expectedSimplifiedPrimaryKeys = Arrays.asList("TEST_TABLE1.COLUMN1", "TEST_TABLE1.COLUMN2",
                "TEST_TABLE3.COLUMN3");
        Assertions.assertEquals(expectedSimplifiedPrimaryKeys, actualSimplifiedPrimaryKeys);

        List<String> actualNonEqualForeignKeys = modelRequest.getSimplifiedJoinTableDescs().stream()
                .map(SimplifiedJoinTableDesc::getSimplifiedJoinDesc)
                .map(SimplifiedJoinDesc::getSimplifiedNonEquiJoinConditions).filter(Objects::nonNull)
                .flatMap(Collection::stream).map(NonEquiJoinCondition.SimplifiedNonEquiJoinCondition::getForeignKey)
                .collect(Collectors.toList());
        List<String> expectedNonEqualForeignKey = Collections.singletonList("TEST_TABLE3.COLUMN2");
        Assertions.assertEquals(expectedNonEqualForeignKey, actualNonEqualForeignKeys);

        List<String> actualNonEqualPrimaryKeys = modelRequest.getSimplifiedJoinTableDescs().stream()
                .map(SimplifiedJoinTableDesc::getSimplifiedJoinDesc)
                .map(SimplifiedJoinDesc::getSimplifiedNonEquiJoinConditions).filter(Objects::nonNull)
                .flatMap(Collection::stream).map(NonEquiJoinCondition.SimplifiedNonEquiJoinCondition::getPrimaryKey)
                .collect(Collectors.toList());
        List<String> expectedNonEqualPrimaryKey = Collections.singletonList("TEST_TABLE3.COLUMN3");
        Assertions.assertEquals(expectedNonEqualPrimaryKey, actualNonEqualPrimaryKeys);
    }

    private void mockSimplifiedDimensions(ModelRequest modelRequest) {
        List<NDataModel.NamedColumn> list = new ArrayList<>();
        {
            val column = new NDataModel.NamedColumn();
            column.setAliasDotColumn("Schema.tesT_tablE.Column1");
            list.add(column);
        }
        {
            val column = new NDataModel.NamedColumn();
            column.setAliasDotColumn("scHema.TEST_TABLE.coluMn2");
            list.add(column);
        }
        {
            val column = new NDataModel.NamedColumn();
            column.setAliasDotColumn("scHema.test_table2.COLUMN");
            list.add(column);
        }
        modelRequest.setSimplifiedDimensions(list);
    }

    private void mockJoinTables(ModelRequest modelRequest) {
        List<JoinTableDesc> joinTables = new ArrayList<>();
        List<SimplifiedJoinTableDesc> simplifiedList = new ArrayList<>();
        {
            val joinTable = new JoinTableDesc();
            JoinDesc joinDesc = new JoinDesc();
            joinTable.setJoin(joinDesc);
            joinTable.setTable("test_table1");
            joinDesc.setPrimaryKey(new String[] { "test_table1.Column1", "test_table1.column2" });
            joinTables.add(joinTable);

            val simplified = new SimplifiedJoinTableDesc();
            simplified.setTable("test_table1");
            SimplifiedJoinDesc simplifiedJoinDesc = new SimplifiedJoinDesc();
            simplified.setSimplifiedJoinDesc(simplifiedJoinDesc);
            simplifiedJoinDesc.setPrimaryKey(new String[] { "test_table1.Column1", "test_table1.column2" });
            simplifiedList.add(simplified);
        }
        {
            val joinTable = new JoinTableDesc();
            JoinDesc joinDesc = new JoinDesc();
            joinTable.setJoin(joinDesc);
            joinTable.setTable("TEST_table2");
            joinDesc.setForeignKey(new String[] { "TEST_table2.column1" });
            joinTables.add(joinTable);

            val simplified = new SimplifiedJoinTableDesc();
            simplified.setTable("TEST_table2");
            SimplifiedJoinDesc simplifiedJoinDesc = new SimplifiedJoinDesc();
            simplified.setSimplifiedJoinDesc(simplifiedJoinDesc);
            simplifiedJoinDesc.setForeignKey(new String[] { "TEST_table2.column1" });
            simplifiedList.add(simplified);
        }
        {
            val joinTable = new JoinTableDesc();
            JoinDesc joinDesc = new JoinDesc();
            joinTable.setJoin(joinDesc);
            joinTable.setTable("TEST_TABLE3");
            joinDesc.setForeignKey(new String[] { "TEST_TABLE3.COLUMN2" });
            joinDesc.setPrimaryKey(new String[] { "TEST_TABLE3.cOLUMn3" });
            joinTables.add(joinTable);

            val simplified = new SimplifiedJoinTableDesc();
            simplified.setTable("TEST_TABLE3");
            SimplifiedJoinDesc simplifiedJoinDesc = new SimplifiedJoinDesc();
            simplified.setSimplifiedJoinDesc(simplifiedJoinDesc);
            simplifiedJoinDesc.setForeignKey(new String[] { "TEST_TABLE3.COLUMN2" });
            simplifiedJoinDesc.setPrimaryKey(new String[] { "TEST_TABLE3.cOLUMn3" });
            simplifiedList.add(simplified);

            val condition = new NonEquiJoinCondition.SimplifiedNonEquiJoinCondition();
            condition.setPrimaryKey("TEST_TABLE3.cOLUMn3");
            condition.setForeignKey("TEST_TABLE3.COLUMN2");
            simplifiedJoinDesc.setSimplifiedNonEquiJoinConditions(Collections.singletonList(condition));
        }
        {
            val joinTable = new JoinTableDesc();
            JoinDesc joinDesc = new JoinDesc();
            joinTable.setJoin(joinDesc);
            joinTable.setTable("Test_table4");
            joinTables.add(joinTable);

            val simplified = new SimplifiedJoinTableDesc();
            simplified.setTable("Test_table4");
            SimplifiedJoinDesc simplifiedJoinDesc = new SimplifiedJoinDesc();
            simplified.setSimplifiedJoinDesc(simplifiedJoinDesc);
            simplifiedList.add(simplified);
        }
        modelRequest.setJoinTables(joinTables);
        modelRequest.setSimplifiedJoinTableDescs(simplifiedList);
    }
}
