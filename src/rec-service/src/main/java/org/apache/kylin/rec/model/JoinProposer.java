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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.kylin.rec.util.TableAliasGenerator;

public class JoinProposer extends AbstractModelProposer {

    public JoinProposer(AbstractContext.ModelContext modelContext) {
        super(modelContext);
    }

    @Override
    public void execute(NDataModel modelDesc) {

        ModelTree modelTree = modelContext.getModelTree();
        Map<String, JoinTableDesc> joinTables = new HashMap<>();

        // step 1. produce unique aliasMap
        TableAliasGenerator.TableAliasDict dict = TableAliasGenerator.generateCommonDictForSpecificModel(project);
        Map<TableRef, String> uniqueTblAliasMap = GreedyModelTreesBuilder.TreeBuilder
                .getUniqueTblAliasBasedOnPosInGraph(modelDesc.getJoinsGraph(), dict);
        for (OlapContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.getJoins().isEmpty() || !isValidOlapContext(ctx)) {
                continue;
            }
            uniqueTblAliasMap.putAll(GreedyModelTreesBuilder.TreeBuilder.getUniqueTblAliasBasedOnPosInGraph(ctx, dict));
        }

        // step 2. correct table alias based on unique aliasMap and keep origin alias unchanged in model
        //                       ***** WARNING ******
        // there is one limitation that suffix of Join table alias is increase by degrees,
        //       just like f_table join lookup join lookup_1 join lookup_2
        // So ERROR Join produced if it needs to change the join relation that join alias is not obey above rule.
        Map<TableRef, String> originTblRefAlias = Maps.newHashMap();
        for (JoinTableDesc joinTable : modelDesc.getJoinTables()) {
            joinTables.put(joinTable.getAlias(), joinTable);
            originTblRefAlias.put(modelDesc.findTable(joinTable.getAlias()), joinTable.getAlias());
        }
        Map<TableRef, String> tableAliasMap = GreedyModelTreesBuilder.TreeBuilder.correctTblAliasAndKeepOriginAlias(
                uniqueTblAliasMap, modelDesc.getRootFactTable().getTableDesc(), originTblRefAlias);

        // step 3. merge context and adjust target model
        Map<String, TableRef> aliasRefMap = Maps.newHashMap();
        for (OlapContext ctx : modelTree.getOlapContexts()) {
            if (ctx == null || ctx.getJoins().isEmpty() || !isValidOlapContext(ctx)) {
                continue;
            }
            try {
                Map<String, JoinTableDesc> tmpJoinTablesMap = new HashMap<>();
                GreedyModelTreesBuilder.TreeBuilder.mergeContext(ctx, tmpJoinTablesMap, tableAliasMap, aliasRefMap);
                tmpJoinTablesMap.forEach((alias, joinTableDesc) -> {
                    JoinTableDesc oldJoinTable = joinTables.get(alias);
                    if (oldJoinTable != null) {
                        String flattenable = oldJoinTable.getFlattenable();
                        joinTableDesc.setFlattenable(flattenable);
                        joinTableDesc.getJoin().setType(oldJoinTable.getJoin().getType());
                    }
                });
                joinTables.putAll(tmpJoinTablesMap);
            } catch (Exception e) {
                Map<String, AccelerateInfo> accelerateInfoMap = modelContext.getProposeContext().getAccelerateInfoMap();
                accelerateInfoMap.get(ctx.getSql()).setFailedCause(e);
            }
        }

        modelDesc.setJoinTables(new ArrayList<>(joinTables.values()));
    }

}
