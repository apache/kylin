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
package org.apache.kylin.rest.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.util.scd2.SCD2Exception;
import org.apache.kylin.metadata.model.util.scd2.SCD2NonEquiCondSimplification;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinDesc;
import org.apache.kylin.metadata.model.util.scd2.SimplifiedJoinTableDesc;

import com.google.common.base.Preconditions;

public class SCD2SimplificationConvertUtil {

    /**
     * fill simplified cond according to joinTables
     * return true if is scd2 cond
     * @param simplifiedJoinTableDescs
     * @param joinTables
     */
    private static void fillSimplifiedCond(@Nonnull List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs,
            @Nonnull List<JoinTableDesc> joinTables) {
        Preconditions.checkNotNull(simplifiedJoinTableDescs);
        Preconditions.checkNotNull(joinTables);

        for (int i = 0; i < joinTables.size(); i++) {
            JoinTableDesc joinTableDesc = joinTables.get(i);
            JoinDesc joinDesc = joinTableDesc.getJoin();

            if (Objects.isNull(joinDesc.getNonEquiJoinCondition())) {
                continue;
            }

            try {
                SimplifiedJoinDesc convertedJoinDesc = SCD2NonEquiCondSimplification.INSTANCE
                        .convertToSimplifiedSCD2Cond(joinDesc);

                SimplifiedJoinDesc responseJoinDesc = simplifiedJoinTableDescs.get(i).getSimplifiedJoinDesc();
                responseJoinDesc
                        .setSimplifiedNonEquiJoinConditions(convertedJoinDesc.getSimplifiedNonEquiJoinConditions());
                responseJoinDesc.setForeignKey(convertedJoinDesc.getForeignKey());
                responseJoinDesc.setPrimaryKey(convertedJoinDesc.getPrimaryKey());
            } catch (SCD2Exception e) {
                throw new KylinException(QueryErrorCode.SCD2_COMMON_ERROR, "only support scd2 join condition");
            }

        }

    }

    /**
     * convert join tables to  simplified join tables,
     * and fill non-equi join cond
     * @param joinTables
     * @return
     */
    public static List<SimplifiedJoinTableDesc> simplifiedJoinTablesConvert(List<JoinTableDesc> joinTables) {
        if (Objects.isNull(joinTables)) {
            return null;
        }

        List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs;
        try {
            simplifiedJoinTableDescs = Arrays.asList(
                    JsonUtil.readValue(JsonUtil.writeValueAsString(joinTables), SimplifiedJoinTableDesc[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        fillSimplifiedCond(simplifiedJoinTableDescs, joinTables);

        return simplifiedJoinTableDescs;

    }

    /**
     * convert simplifiedJoinTables to join tables
     * @param simplifiedJoinTableDescs
     * @return
     */
    public static List<JoinTableDesc> convertSimplified2JoinTables(
            List<SimplifiedJoinTableDesc> simplifiedJoinTableDescs) {
        if (Objects.isNull(simplifiedJoinTableDescs)) {
            return null;
        }
        List<JoinTableDesc> joinTableDescs;
        try {
            joinTableDescs = Arrays.asList(
                    JsonUtil.readValue(JsonUtil.writeValueAsString(simplifiedJoinTableDescs), JoinTableDesc[].class));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return joinTableDescs;

    }

    public static List<JoinTableDesc> deepCopyJoinTables(List<JoinTableDesc> joinTables) {

        return convertSimplified2JoinTables(simplifiedJoinTablesConvert(joinTables));

    }
}
