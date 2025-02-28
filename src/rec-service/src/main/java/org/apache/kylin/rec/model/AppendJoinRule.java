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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;

public class AppendJoinRule extends AbstractJoinRule {
    /*
     * Compatible conditions:
     *  1. same fact table,
     *  2. each join is not one-to-many or many-to-many relation,
     *  3. each join from fact table is left-join,
     *  4. each join is not non-equiv-join
     */
    @Override
    public boolean isCompatible(NDataModel model, ModelTree modelTree) {

        String factTableName = model.getRootFactTableName();
        if (!StringUtils.equalsIgnoreCase(factTableName, modelTree.getRootFactTable().getIdentity())) {
            return false;
        }

        for (JoinTableDesc joinTable : model.getJoinTables()) {
            if (detectIncompatible(factTableName, joinTable)) {
                return false;
            }
        }

        for (Map.Entry<String, JoinTableDesc> joinEntry : modelTree.getJoins().entrySet()) {
            if (detectIncompatible(factTableName, joinEntry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private boolean detectIncompatible(String factTableName, JoinTableDesc joinTable) {
        JoinDesc join = joinTable.getJoin();
        if (join.isNonEquiJoin()) {
            return true;
        }

        return join.isJoinWithFactTable(factTableName) && (joinTable.isToManyJoinRelation() || !join.isLeftJoin());
    }
}
