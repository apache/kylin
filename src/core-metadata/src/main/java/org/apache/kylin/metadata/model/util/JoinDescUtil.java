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

package org.apache.kylin.metadata.model.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.NDataModel.TableKind;

public class JoinDescUtil {

    private JoinDescUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static JoinTableDesc convert(JoinDesc join, TableKind kind, String pkTblAlias, String fkTblAlias,
            Map<String, TableRef> aliasTableRefMap) {
        if (join == null) {
            return null;
        }

        TableRef table = join.getPKSide();
        JoinTableDesc joinTableDesc = new JoinTableDesc();
        joinTableDesc.setKind(kind);
        joinTableDesc.setTable(table.getTableIdentity());
        joinTableDesc.setAlias(pkTblAlias);

        JoinDesc.JoinDescBuilder joinDescBuilder = new JoinDesc.JoinDescBuilder();

        joinDescBuilder.setType(join.getType());
        joinDescBuilder.setLeftOrInner(join.isLeftOrInnerJoin());
        String[] pkCols = new String[join.getPrimaryKey().length];
        TblColRef[] pkColRefs = new TblColRef[pkCols.length];

        TableRef pkTblRef = aliasTableRefMap.computeIfAbsent(pkTblAlias,
                alias -> TblColRef.tableForUnknownModel(alias, join.getPKSide().getTableDesc()));
        for (int i = 0; i < pkCols.length; i++) {
            TblColRef colRef = join.getPrimaryKeyColumns()[i];
            pkCols[i] = pkTblAlias + "." + colRef.getName();
            pkColRefs[i] = TblColRef.columnForUnknownModel(pkTblRef, colRef.getColumnDesc());
        }
        joinDescBuilder.addPrimaryKeys(pkCols, pkColRefs);
        joinDescBuilder.setPrimaryTableRef(pkTblRef);

        String[] fkCols = new String[join.getForeignKey().length];
        TblColRef[] fkColRefs = new TblColRef[fkCols.length];

        TableRef fkTblRef = aliasTableRefMap.computeIfAbsent(fkTblAlias,
                alias -> TblColRef.tableForUnknownModel(alias, join.getFKSide().getTableDesc()));
        for (int i = 0; i < fkCols.length; i++) {
            TblColRef colRef = join.getForeignKeyColumns()[i];
            fkCols[i] = fkTblAlias + "." + colRef.getName();
            fkColRefs[i] = TblColRef.columnForUnknownModel(fkTblRef, colRef.getColumnDesc());
        }
        joinDescBuilder.addForeignKeys(fkCols, fkColRefs);
        joinDescBuilder.setForeignTableRef(fkTblRef);

        if (join.getNonEquiJoinCondition() != null) {
            NonEquiJoinCondition nonEquiJoinCondition = convertNonEquiJoinCondition(join.getNonEquiJoinCondition(),
                    pkTblRef, fkTblRef);
            String expr = join.getNonEquiJoinCondition().getExpr();
            expr = expr.replaceAll(join.getPKSide().getAlias(), pkTblAlias);
            expr = expr.replaceAll(join.getFKSide().getAlias(), fkTblAlias);
            nonEquiJoinCondition.setExpr(expr);
            joinDescBuilder.setNonEquiJoinCondition(nonEquiJoinCondition);
        }
        joinTableDesc.setJoin(joinDescBuilder.build());

        return joinTableDesc;
    }

    private static NonEquiJoinCondition convertNonEquiJoinCondition(NonEquiJoinCondition cond, TableRef pkTblRef,
            TableRef fkTblRef) {
        if (cond.getType() == NonEquiJoinConditionType.EXPRESSION) {
            return new NonEquiJoinCondition(cond.getOpName(), cond.getOp(),
                    Arrays.stream(cond.getOperands())
                            .map(condInput -> convertNonEquiJoinCondition(condInput, pkTblRef, fkTblRef))
                            .toArray(NonEquiJoinCondition[]::new),
                    cond.getDataType());
        } else if (cond.getType() == NonEquiJoinConditionType.LITERAL) {
            return cond;
        } else {
            return new NonEquiJoinCondition(convertColumn(cond.getColRef(), pkTblRef, fkTblRef), cond.getDataType());
        }
    }

    private static TblColRef convertColumn(TblColRef colRef, TableRef pkTblRef, TableRef fkTblRef) {
        if (colRef.getTableRef().getTableIdentity().equals(pkTblRef.getTableIdentity())) {
            return TblColRef.columnForUnknownModel(pkTblRef, colRef.getColumnDesc());
        } else {
            return TblColRef.columnForUnknownModel(fkTblRef, colRef.getColumnDesc());
        }
    }

    public static List<Pair<JoinDesc, TableKind>> resolveTableType(List<JoinDesc> joins) {
        List<Pair<JoinDesc, TableKind>> tableKindByJoins = new ArrayList<>();
        Map<String, JoinDesc> fkTables = new HashMap<>();
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getFKSide();
            String tableAlias = table.getAlias();
            if (fkTables.containsKey(tableAlias)) {
                // error
            }
            fkTables.put(tableAlias, joinDesc);
        }
        for (JoinDesc joinDesc : joins) {
            TableRef table = joinDesc.getPKSide();
            String tableAlias = table.getAlias();
            //            if (fkTables.containsKey(tableAlias)) {
            tableKindByJoins.add(new Pair<JoinDesc, TableKind>(joinDesc, TableKind.FACT));
            //            } else {
            //            tableKindByJoins.add(new Pair<JoinDesc, TableKind>(joinDesc, TableKind.LOOKUP));
            //            }
        }
        return tableKindByJoins;
    }

    public static boolean isJoinTypeEqual(JoinDesc a, JoinDesc b) {
        return (a.isInnerJoin() && b.isInnerJoin()) || (a.isLeftJoin() && b.isLeftJoin());
    }

    public static boolean isJoinTableEqual(JoinTableDesc a, JoinTableDesc b) {
        if (a == b)
            return true;

        if (!a.getTable().equalsIgnoreCase(b.getTable()))
            return false;
        if (a.getKind() != b.getKind())
            return false;
        if (!a.getAlias().equalsIgnoreCase(b.getAlias()))
            return false;

        JoinDesc ja = a.getJoin();
        JoinDesc jb = b.getJoin();
        if (!ja.getType().equalsIgnoreCase(jb.getType()))
            return false;
        if (!Arrays.equals(ja.getForeignKey(), jb.getForeignKey()))
            return false;
        if (!Arrays.equals(ja.getPrimaryKey(), jb.getPrimaryKey()))
            return false;
        if (!Objects.equals(ja.getNonEquiJoinCondition(), jb.getNonEquiJoinCondition())) {
            return false;
        }
        return true;
    }

    public static boolean isJoinKeysEqual(JoinDesc a, JoinDesc b) {
        if (!Arrays.equals(a.getForeignKey(), b.getForeignKey()))
            return false;
        if (!Arrays.equals(a.getPrimaryKey(), b.getPrimaryKey()))
            return false;
        return true;
    }

    public static String toString(JoinDesc join) {
        StringBuilder result = new StringBuilder();
        result.append(" ").append(join.getFKSide().getTableIdentity()).append(" AS ")
                .append(join.getFKSide().getAlias()).append(" ").append(join.getType()).append(" JOIN ")
                .append(join.getPKSide().getTableIdentity()).append(" AS ").append(join.getPKSide().getAlias())
                .append(" ON ");
        for (int i = 0; i < join.getForeignKey().length; i++) {
            String fk = join.getForeignKey()[i];
            String pk = join.getPrimaryKey()[i];
            if (i > 0) {
                result.append(" AND ");
            }
            result.append(fk).append("=").append(pk);
        }
        return result.toString();
    }
}
