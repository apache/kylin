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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.var;

public class AbstractModelService extends BasicService {

    public static final String VALID_NAME_FOR_MODEL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_";

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

}
