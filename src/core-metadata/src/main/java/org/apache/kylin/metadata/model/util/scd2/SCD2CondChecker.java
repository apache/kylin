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
package org.apache.kylin.metadata.model.util.scd2;

import static org.apache.kylin.metadata.model.NonEquiJoinCondition.SimplifiedNonEquiJoinCondition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.NDataModel;

/**
 * do something check on scd2 condition
 */
public class SCD2CondChecker {

    public static final SCD2CondChecker INSTANCE = new SCD2CondChecker();

    /**
     * SCD2 must have  equi condition
     */
    public boolean checkSCD2EquiJoinCond(String[] fks, String[] pks) {
        return fks.length > 0 && fks.length == pks.length;
    }

    public boolean isScd2Model(NDataModel nDataModel) {
        if (nDataModel.getJoinTables().stream().filter(joinTableDesc -> joinTableDesc.getJoin().isNonEquiJoin())
                .count() == 0) {
            return false;
        }

        return nDataModel.getJoinTables().stream().filter(joinTableDesc -> joinTableDesc.getJoin().isNonEquiJoin())
                .allMatch(joinTableDesc -> SCD2NonEquiCondSimplification.INSTANCE
                        .simplifiedSCD2CondConvertChecker(joinTableDesc.getJoin()));
    }

    public boolean checkFkPkPairUnique(SimplifiedJoinDesc joinDesc) {
        return checkFkPkPairUnique(SCD2NonEquiCondSimplification.INSTANCE.simplifyFksPks(joinDesc.getForeignKey(),
                joinDesc.getPrimaryKey()), joinDesc.getSimplifiedNonEquiJoinConditions());
    }

    public boolean checkSCD2NonEquiJoinCondPair(final List<SimplifiedNonEquiJoinCondition> simplified) {
        if (CollectionUtils.isEmpty(simplified)) {
            return false;
        }

        int size = simplified.size();

        if (size % 2 != 0) {
            return false;
        }

        Map<String, Integer> mappingCount = new HashMap<>();

        for (SimplifiedNonEquiJoinCondition cond : simplified) {
            if (!checkSCD2SqlOp(cond.getOp())) {
                return false;
            }

            int inrc = cond.getOp() == SqlKind.GREATER_THAN_OR_EQUAL ? 1 : -1;

            mappingCount.put(cond.getForeignKey(), mappingCount.getOrDefault(cond.getForeignKey(), 0) + inrc);

        }

        return !mappingCount.values().stream().anyMatch(count -> count != 0);

    }

    boolean checkFkPkPairUnique(List<SimplifiedNonEquiJoinCondition> equiFkPks,
            List<SimplifiedNonEquiJoinCondition> nonEquiFkPks) {
        List<SimplifiedNonEquiJoinCondition> allFkPks = new ArrayList<>();
        allFkPks.addAll(equiFkPks);
        allFkPks.addAll(nonEquiFkPks);

        HashSet pairSet = new HashSet<String>();

        for (int i = 0; i < allFkPks.size(); i++) {
            String key = allFkPks.get(i).getForeignKey() + allFkPks.get(i).getPrimaryKey();
            if (pairSet.contains(key)) {
                return false;
            }
            pairSet.add(key);

        }
        return true;

    }

    /**
     * SCD2 only support =, >=, <
     * @param op
     * @return
     */
    private boolean checkSCD2SqlOp(SqlKind op) {

        if (Objects.isNull(op)) {
            return false;
        }

        switch (op) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
            return true;
        default:
            return false;
        }

    }
}
