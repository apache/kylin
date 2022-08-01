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

import java.util.HashMap;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

public class ModelNonEquiCondMock {

    private HashMap<String, TableRef> tableRefCache = new HashMap<>();

    public void clearTableRefCache() {
        tableRefCache.clear();
    }

    public NonEquiJoinCondition mockTblColRefCond(String tableCol, SqlTypeName colType) {
        int idxTableEnd = tableCol.indexOf('.');
        TblColRef colRef = mockTblColRef(tableCol.substring(0, idxTableEnd), tableCol.substring(idxTableEnd + 1));
        return mockTblColRefCond(colRef, colType);
    }

    public TableRef mockTblRef(String table) {
        if (!tableRefCache.containsKey(table)) {
            TableDesc tableDesc = new TableDesc();
            tableDesc.setName("DUMMY." + table);
            tableDesc.setColumns(new ColumnDesc[0]);
            tableRefCache.put(table, TblColRef.tableForUnknownModel(table, tableDesc));
        }
        return tableRefCache.get(table);
    }

    public TblColRef mockTblColRef(String table, String col) {
        return mockTblRef(table).makeFakeColumn(col);
    }

    public NonEquiJoinCondition colConstantCompareCond(SqlKind op, String col1, SqlTypeName col1Type,
            String constantValue, SqlTypeName constantType) {
        return new NonEquiJoinCondition(op.sql, op, new NonEquiJoinCondition[] { mockTblColRefCond(col1, col1Type),
                mockConstantCond(constantValue, constantType) }, mockDataType(SqlTypeName.BOOLEAN));
    }

    public NonEquiJoinCondition colOp(SqlKind op, String col1) {
        return new NonEquiJoinCondition(op.sql, op,
                new NonEquiJoinCondition[] { mockTblColRefCond(col1, SqlTypeName.CHAR) },
                mockDataType(SqlTypeName.BOOLEAN));
    }

    public NonEquiJoinCondition colOp(String sqlName, SqlKind op, String col1) {
        return new NonEquiJoinCondition(sqlName, op,
                new NonEquiJoinCondition[] { mockTblColRefCond(col1, SqlTypeName.CHAR) },
                mockDataType(SqlTypeName.BOOLEAN));
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, SqlTypeName col1Type, String col2,
            SqlTypeName col2Type) {
        return new NonEquiJoinCondition(op.sql, op,
                new NonEquiJoinCondition[] { mockTblColRefCond(col1, col1Type), mockTblColRefCond(col2, col2Type) },
                mockDataType(SqlTypeName.BOOLEAN));
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, String col2, SqlTypeName typeName) {
        return colCompareCond(op, col1, typeName, col2, typeName);
    }

    public NonEquiJoinCondition colCompareCond(SqlKind op, String col1, String col2) {
        return colCompareCond(op, col1, col2, SqlTypeName.CHAR);
    }

    public static NonEquiJoinCondition composite(SqlKind op, NonEquiJoinCondition... conds) {
        return new NonEquiJoinCondition(op.sql, op, conds, mockDataType(SqlTypeName.BOOLEAN));
    }

    public static NonEquiJoinCondition mockTblColRefCond(TblColRef tableCol, SqlTypeName colType) {
        return new NonEquiJoinCondition(tableCol, mockDataType(colType));
    }

    public static NonEquiJoinCondition mockConstantCond(String constantValue, SqlTypeName constantType) {
        return new NonEquiJoinCondition(constantValue, mockDataType(constantType));
    }

    public static DataType mockDataType(SqlTypeName sqlTypeName) {
        DataType dataType = new DataType();
        dataType.setTypeName(sqlTypeName);
        return dataType;
    }
}
