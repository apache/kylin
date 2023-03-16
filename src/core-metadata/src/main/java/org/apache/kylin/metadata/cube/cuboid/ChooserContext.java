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
package org.apache.kylin.metadata.cube.cuboid;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMultimap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.Getter;
import lombok.val;

@Getter
public class ChooserContext {

    final NDataModel model;

    final ImmutableMultimap<Integer, Integer> fk2Pk;
    final Map<TblColRef, Integer> tblColMap = Maps.newHashMap();
    final Map<String, List<Integer>> primaryKeyColumnIds = Maps.newHashMap();
    final Map<String, List<Integer>> foreignKeyColumnIds = Maps.newHashMap();
    final Map<Integer, TableExtDesc.ColumnStats> columnStatMap = Maps.newHashMap();

    public ChooserContext(NDataModel model) {
        this.model = model;

        ImmutableMultimap.Builder<Integer, Integer> fk2PkBuilder = ImmutableMultimap.builder();

        initModelContext(model, fk2PkBuilder);
        if (isBatchFusionModel()) {
            NDataModel streamingModel = NDataModelManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                    .getDataModelDesc(model.getFusionId());
            initModelContext(streamingModel, fk2PkBuilder);
        }

        this.fk2Pk = fk2PkBuilder.build();

    }

    public TableExtDesc.ColumnStats getColumnStats(TblColRef ref) {
        int colId = tblColMap.getOrDefault(ref, -1);
        return columnStatMap.get(colId);
    }

    public TblColRef convertToRef(Integer colId) {
        return model.getEffectiveCols().get(colId);
    }

    public Collection<TblColRef> convertToRefs(Collection<Integer> colIds) {
        List<TblColRef> refs = Lists.newArrayList();
        for (Integer colId : colIds) {
            refs.add(convertToRef(colId));
        }
        return refs;
    }

    public boolean isBatchFusionModel() {
        return model.isFusionModel() && !model.isStreaming();
    }

    private void initModelContext(NDataModel dataModel, ImmutableMultimap.Builder<Integer, Integer> fk2PkBuilder) {
        val effectiveCols = dataModel.getEffectiveCols();
        effectiveCols.forEach((key, value) -> tblColMap.put(value, key));

        val config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tblMetaMgr = NTableMetadataManager.getInstance(config, model.getProject());
        effectiveCols.keySet().forEach(colId -> {
            TableExtDesc.ColumnStats stats = TableExtDesc.ColumnStats.getColumnStats(tblMetaMgr,
                    effectiveCols.get(colId));
            columnStatMap.put(colId, stats);
        });

        if (CollectionUtils.isEmpty(dataModel.getJoinTables())) {
            return;
        }
        dataModel.getJoinTables().forEach(joinDesc -> {
            List<Integer> pks = Lists.newArrayList();
            List<Integer> fks = Lists.newArrayList();
            primaryKeyColumnIds.put(joinDesc.getAlias(), Arrays.stream(joinDesc.getJoin().getPrimaryKeyColumns())
                    .map(tblColMap::get).collect(Collectors.toList()));
            foreignKeyColumnIds.put(joinDesc.getAlias(), Arrays.stream(joinDesc.getJoin().getForeignKeyColumns())
                    .map(tblColMap::get).collect(Collectors.toList()));

            val join = joinDesc.getJoin();
            int n = join.getForeignKeyColumns().length;
            for (int i = 0; i < n; i++) {
                val pk = join.getPrimaryKeyColumns()[i];
                val fk = join.getForeignKeyColumns()[i];
                val pkId = tblColMap.get(pk);
                val fkId = tblColMap.get(fk);
                pks.add(pkId);
                fks.add(fkId);
                fk2PkBuilder.put(fkId, pkId);
            }
            primaryKeyColumnIds.put(joinDesc.getAlias(), pks);
            foreignKeyColumnIds.put(joinDesc.getAlias(), fks);
        });
    }

}
