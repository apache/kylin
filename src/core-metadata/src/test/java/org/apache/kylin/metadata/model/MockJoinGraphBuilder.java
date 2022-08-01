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

package org.apache.kylin.metadata.model;

import java.util.List;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;

import com.google.common.collect.Lists;

public class MockJoinGraphBuilder {
    private NDataModel modelDesc;
    private TableRef root;
    private List<JoinDesc> joins;

    public MockJoinGraphBuilder(NDataModel modelDesc, String rootName) {
        this.modelDesc = modelDesc;
        this.root = modelDesc.findTable(rootName);
        Assert.assertNotNull(root);
        this.joins = Lists.newArrayList();
    }

    private JoinDesc mockJoinDesc(String joinType, String[] fkCols, String[] pkCols) {
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType(joinType);
        joinDesc.setPrimaryKey(fkCols);
        joinDesc.setPrimaryKey(pkCols);
        TblColRef[] fkColRefs = new TblColRef[fkCols.length];
        for (int i = 0; i < fkCols.length; i++) {
            fkColRefs[i] = modelDesc.findColumn(fkCols[i]);
        }
        TblColRef[] pkColRefs = new TblColRef[pkCols.length];
        for (int i = 0; i < pkCols.length; i++) {
            pkColRefs[i] = modelDesc.findColumn(pkCols[i]);
        }
        joinDesc.setForeignKeyColumns(fkColRefs);
        joinDesc.setPrimaryKeyColumns(pkColRefs);
        return joinDesc;
    }

    public MockJoinGraphBuilder innerJoin(String[] fkCols, String[] pkCols) {
        joins.add(mockJoinDesc(JoinType.INNER.name(), fkCols, pkCols));
        return this;
    }

    public MockJoinGraphBuilder leftJoin(String[] fkCols, String[] pkCols) {
        joins.add(mockJoinDesc(JoinType.LEFT.name(), fkCols, pkCols));
        return this;
    }

    public MockJoinGraphBuilder nonEquiLeftJoin(String pkTblName, String fkTblName, String nonEquiCol) {
        return nonEquiJoinByJoinType(JoinType.LEFT, pkTblName, fkTblName, nonEquiCol);
    }

    public MockJoinGraphBuilder nonEquiInnerJoin(String pkTblName, String fkTblName, String nonEquiCol) {
        return nonEquiJoinByJoinType(JoinType.INNER, pkTblName, fkTblName, nonEquiCol);
    }

    // simply add a col=constant condition to make a join cond non-equi
    private MockJoinGraphBuilder nonEquiJoinByJoinType(JoinType joinType, String pkTblName, String fkTblName,
            String nonEquiCol) {
        int idxTableEnd = nonEquiCol.indexOf('.');
        TableRef tableRef = modelDesc.findTable(nonEquiCol.substring(0, idxTableEnd));
        TblColRef tblColRef = tableRef.getColumn(nonEquiCol.substring(idxTableEnd + 1));
        JoinDesc joinDesc = mockJoinDesc(joinType.name(), new String[0], new String[0]);
        joinDesc.setPrimaryTableRef(modelDesc.findTable(pkTblName));
        joinDesc.setForeignTableRef(modelDesc.findTable(fkTblName));
        joinDesc.setNonEquiJoinCondition(ModelNonEquiCondMock.composite(SqlKind.EQUALS,
                ModelNonEquiCondMock.mockTblColRefCond(tblColRef, SqlTypeName.CHAR),
                ModelNonEquiCondMock.mockConstantCond("DUMMY", SqlTypeName.CHAR)));
        joins.add(joinDesc);
        return this;
    }

    public JoinsGraph build() {
        return new JoinsGraph(root, joins);
    }
}
