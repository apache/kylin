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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_UPDATE_MODEL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_ID_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_INVALID;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NAME_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_NOT_EXIST;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AbstractModelService extends BasicService {

    public static final String VALID_NAME_FOR_MODEL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";
    protected static final String SAVE_INDEXES_STRATEGY = "single_dim_and_reduce_hc";

    @Autowired
    public AclEvaluate aclEvaluate;

    @Autowired
    public AccessService accessService;

    public void checkModelPermission(String project, String modelId) {
        String userName = aclEvaluate.getCurrentUserName();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.isAdmin() || AclPermissionUtil.isAdminInProject(project, groups)) {
            return;
        }
        Set<String> allAuthTables = Sets.newHashSet();
        Set<String> allAuthColumns = Sets.newHashSet();
        var auths = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, userName, true);
        allAuthTables.addAll(auths.getTables());
        allAuthColumns.addAll(auths.getColumns());
        for (val group : groups) {
            auths = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, group, false);
            allAuthTables.addAll(auths.getTables());
            allAuthColumns.addAll(auths.getColumns());
        }

        NDataModel model = getModelById(modelId, project);
        Set<String> tablesInModel = Sets.newHashSet();
        model.getJoinTables().forEach(table -> tablesInModel.add(table.getTable()));
        tablesInModel.add(model.getRootFactTableName());
        tablesInModel.forEach(table -> {
            if (!allAuthTables.contains(table)) {
                throw new KylinException(FAILED_UPDATE_MODEL, MsgPicker.getMsg().getModelModifyAbandon(table));
            }
        });
        tablesInModel.stream().filter(allAuthTables::contains).forEach(table -> {
            ColumnDesc[] columnDescs = NTableMetadataManager.getInstance(getConfig(), project).getTableDesc(table)
                    .getColumns();
            Arrays.stream(columnDescs).map(column -> table + "." + column.getName()).forEach(column -> {
                if (!allAuthColumns.contains(column)) {
                    throw new KylinException(FAILED_UPDATE_MODEL, MsgPicker.getMsg().getModelModifyAbandon(column));
                }
            });
        });
    }

    public NDataModel getModelById(String modelId, String project) {
        NDataModelManager modelManager = getManager(NDataModelManager.class, project);
        NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
        if (null == nDataModel) {
            throw new KylinException(MODEL_ID_NOT_EXIST, modelId);
        }
        return nDataModel;
    }

    public NDataModel getModelByAlias(String modelAlias, String project) {
        NDataModelManager modelManager = getManager(NDataModelManager.class, project);
        NDataModel nDataModel = modelManager.getDataModelDescByAlias(modelAlias);
        if (null == nDataModel) {
            throw new KylinException(MODEL_NAME_NOT_EXIST, modelAlias);
        }
        return nDataModel;
    }

    public Set<String> listAllModelIdsInProject(String project) {
        NDataModelManager dataModelManager = getManager(NDataModelManager.class, project);
        return dataModelManager.listAllModelIds();
    }

    public IndexPlan getIndexPlan(String modelId, String project) {
        NIndexPlanManager indexPlanManager = getManager(NIndexPlanManager.class, project);
        return indexPlanManager.getIndexPlan(modelId);
    }

    public void primaryCheck(NDataModel modelDesc) {
        if (modelDesc == null) {
            throw new KylinException(MODEL_NOT_EXIST);
        }

        String modelAlias = modelDesc.getAlias();

        if (StringUtils.isEmpty(modelAlias)) {
            throw new KylinException(MODEL_NAME_EMPTY);
        }
        if (!StringUtils.containsOnly(modelAlias, VALID_NAME_FOR_MODEL)) {
            throw new KylinException(MODEL_NAME_INVALID, modelAlias);
        }
    }

    private boolean needToDeleteLayout(NTableMetadataManager tableManager, NDataModel model, IndexEntity index) {
        int dimSize = index.getDimensions().size();
        if (dimSize > 1) {
            return true;
        }

        if (dimSize == 0) {
            return false;
        }

        return tableManager.isHighCardinalityDim(model.getColRef(index.getDimensions().get(0)));
    }

    private Pair<Boolean, Integer> processIndex(NDataModel model, IndexEntity index,
            IndexPlan.IndexPlanUpdateHandler updateHandler, NTableMetadataManager tableManager) {
        boolean needSplit = true;
        Integer shardByCol = null;

        List<LayoutEntity> layouts = index.getLayouts();
        for (int i = layouts.size() - 1; i >= 0; i--) {
            LayoutEntity layout = layouts.get(i);

            // When the recommended index and agg index overlap,
            // there will be two layouts under this index, and they are only in different col order.
            // This index does not require split.
            if (layout.isBaseIndex()) {
                needSplit = false;
                continue;
            }

            List<Integer> shardByCols = layout.getShardByColumns();
            if (CollectionUtils.isNotEmpty(shardByCols)) {
                shardByCol = shardByCols.get(0);
            }

            if (needToDeleteLayout(tableManager, model, index)) {
                updateHandler.remove(layouts.get(i), IndexEntity.isAggIndex(index.getId()), false, false);
            }
        }

        return Pair.newPair(needSplit, shardByCol);
    }

    protected void splitIndexesIntoSingleDimIndexes(NDataModel model, IndexPlan indexPlan) {
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, model.getProject());
        IndexPlan.IndexPlanUpdateHandler updateHandler = indexPlan.createUpdateHandler();
        List<IndexEntity> indexes = indexPlan.getIndexes();
        for (IndexEntity index : indexes) {
            Pair<Boolean, Integer> needSplitAndShardByCol = processIndex(model, index, updateHandler, tableManager);
            boolean needSplit = needSplitAndShardByCol.getFirst();
            Integer shardByCol = needSplitAndShardByCol.getSecond();

            if (!needSplit) {
                continue;
            }

            List<Integer> measures = index.getMeasures();
            for (Integer dimension : index.getDimensions()) {
                TblColRef colRef = model.getColRef(dimension);
                if (tableManager.isHighCardinalityDim(colRef)) {
                    log.warn("The col {} is high cardinality dimension, not recommended as an index", colRef.getName());
                    continue;
                }

                List<Integer> colOrder = Lists.newArrayList();
                colOrder.add(dimension);
                boolean containShardByCol = colOrder.contains(shardByCol);
                colOrder.addAll(measures);

                LayoutEntity singleDimensionAggLayout;
                if (containShardByCol) {
                    singleDimensionAggLayout = indexPlan.createLayout(colOrder, true, false,
                            Lists.newArrayList(shardByCol));
                } else {
                    singleDimensionAggLayout = indexPlan.createLayout(colOrder, true, false, Lists.newArrayList());
                }

                updateHandler.add(singleDimensionAggLayout, true, true);
            }
        }

        updateHandler.complete();
    }

}
