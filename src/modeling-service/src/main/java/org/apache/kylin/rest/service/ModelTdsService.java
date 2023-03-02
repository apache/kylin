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

import static org.apache.kylin.common.exception.ServerErrorCode.MODEL_BROKEN;
import static org.apache.kylin.common.exception.ServerErrorCode.UNAUTHORIZED_ENTITY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_TDS_EXPORT_COLUMN_AND_MEASURE_NAME_CONFLICT;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.MODEL_TDS_EXPORT_DIM_COL_AND_MEASURE_NAME_CONFLICT;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.engine.spark.smarter.IndexDependencyParser;
import org.apache.kylin.metadata.acl.AclTCRDigest;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.bisync.BISyncModel;
import org.apache.kylin.tool.bisync.BISyncTool;
import org.apache.kylin.tool.bisync.SyncContext;
import org.apache.kylin.tool.bisync.SyncModelBuilder;
import org.apache.kylin.tool.bisync.model.ColumnDef;
import org.apache.kylin.tool.bisync.model.MeasureDef;
import org.apache.kylin.tool.bisync.model.SyncModel;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("modelTdsService")
public class ModelTdsService extends AbstractModelService {

    public void dumpSyncModel(SyncContext syncContext, SyncModel syncModel, HttpServletResponse response)
            throws IOException {
        String projectName = syncContext.getProjectName();
        String modelId = syncContext.getModelId();
        SyncContext.BI exportAs = syncContext.getTargetBI();
        BISyncModel biSyncModel = BISyncTool.getBISyncModel(syncContext, syncModel);

        NDataModelManager manager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        NDataModel dataModel = manager.getDataModelDesc(modelId);
        String alias = dataModel.getAlias();
        String fileName = String.format(Locale.ROOT, "%s_%s_%s", projectName, alias,
                new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault(Locale.Category.FORMAT)).format(new Date()));
        switch (exportAs) {
        case TABLEAU_CONNECTOR_TDS:
        case TABLEAU_ODBC_TDS:
            response.setContentType("application/xml");
            response.setHeader("Content-Disposition",
                    String.format(Locale.ROOT, "attachment; filename=\"%s.tds\"", fileName));
            break;
        default:
            throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, "unrecognized export target");
        }
        biSyncModel.dump(response.getOutputStream());
        response.getOutputStream().flush();
        response.getOutputStream().close();
    }

    public boolean preCheckNameConflict(SyncModel syncModel) {
        boolean skipCheckTds = NProjectManager.getProjectConfig(syncModel.getProject()).skipCheckTds();

        if (!skipCheckTds) {
            Set<String> measureNames = syncModel.getMetrics().stream().filter(measureDef -> !measureDef.isHidden())
                    .map(measureDef -> measureDef.getMeasure().getName()).collect(Collectors.toSet());
            Map<String, ColumnDef> nameOfColDefMap = syncModel.getColumnDefMap().values().stream()
                    .collect(Collectors.toMap(ColumnDef::getAliasDotColumn, Function.identity()));

            nameOfColDefMap.forEach((aliasColName, columnDef) -> {
                String name = aliasColName.split("\\.").length > 1 ? aliasColName.split("\\.")[1] : "";
                if (measureNames.contains(name)) {
                    if (columnDef.isDimension()) {
                        throw new KylinException(MODEL_TDS_EXPORT_DIM_COL_AND_MEASURE_NAME_CONFLICT, name, name);
                    } else {
                        throw new KylinException(MODEL_TDS_EXPORT_COLUMN_AND_MEASURE_NAME_CONFLICT, name, name);
                    }
                }
            });
        }
        return true;
    }

    public SyncModel exportModel(SyncContext syncContext) {
        if (AclPermissionUtil.isAdmin()) {
            return exportTDSDimensionsAndMeasuresByAdmin(syncContext, ImmutableList.of(), ImmutableList.of());
        }

        String projectName = syncContext.getProjectName();
        String modelId = syncContext.getModelId();
        checkModelExportPermission(projectName, modelId);
        checkModelPermission(projectName, modelId);
        return exportTDSDimensionsAndMeasuresByNormalUser(syncContext, ImmutableList.of(), ImmutableList.of());
    }

    public SyncModel exportTDSDimensionsAndMeasuresByNormalUser(SyncContext syncContext, List<String> dimensions,
            List<String> measures) {
        String project = syncContext.getProjectName();
        String modelId = syncContext.getModelId();

        Set<String> authTables = Sets.newHashSet();
        Set<String> authColumns = Sets.newHashSet();
        AclTCRManager aclMgr = getManager(AclTCRManager.class, project);
        AclTCRDigest uDigest = aclMgr.getAuthTablesAndColumns(project, aclEvaluate.getCurrentUserName(), true);
        authTables.addAll(uDigest.getTables());
        authColumns.addAll(uDigest.getColumns());
        getCurrentUserGroups().forEach(group -> {
            AclTCRDigest gDigest = aclMgr.getAuthTablesAndColumns(project, group, false);
            authTables.addAll(gDigest.getTables());
            authColumns.addAll(gDigest.getColumns());
        });

        Set<String> authorizedCols = Sets.newHashSet();
        getModelById(modelId, project).getAllTables().forEach(tableRef -> {
            List<String> colIdentityList = tableRef.getColumns().stream()
                    .filter(colRef -> authColumns.contains(colRef.getCanonicalName())) //
                    .map(TblColRef::getAliasDotName) //
                    .collect(Collectors.toList());
            authorizedCols.addAll(colIdentityList);
        });

        checkTableHasColumnPermission(syncContext.getModelElement(), project, modelId, authorizedCols, dimensions,
                measures);
        return new SyncModelBuilder(syncContext).buildHasPermissionSourceSyncModel(authTables, authorizedCols,
                dimensions, measures);
    }

    public SyncModel exportTDSDimensionsAndMeasuresByAdmin(SyncContext syncContext, List<String> dimensions,
            List<String> measures) {
        return new SyncModelBuilder(syncContext).buildSourceSyncModel(dimensions, measures);
    }

    private void checkBrokenModel(String projectName, String modelId) {
        NDataflow dataflow = getManager(NDataflowManager.class, projectName).getDataflow(modelId);
        if (dataflow.getStatus() == RealizationStatusEnum.BROKEN) {
            throw new KylinException(MODEL_BROKEN, "The model is broken and cannot be exported TDS file");
        }
    }

    public SyncContext prepareSyncContext(String projectName, String modelId, SyncContext.BI targetBI,
            SyncContext.ModelElement modelElement, String host, int port) {
        checkBrokenModel(projectName, modelId);
        SyncContext syncContext = new SyncContext();
        syncContext.setProjectName(projectName);
        syncContext.setModelId(modelId);
        syncContext.setTargetBI(targetBI);
        syncContext.setModelElement(modelElement);
        syncContext.setHost(host);
        syncContext.setPort(port);
        syncContext.setAdmin(AclPermissionUtil.isAdmin());
        syncContext.setDataflow(getManager(NDataflowManager.class, projectName).getDataflow(modelId));
        syncContext.setKylinConfig(getManager(NProjectManager.class).getProject(projectName).getConfig());
        return syncContext;
    }

    public void checkTableHasColumnPermission(SyncContext.ModelElement modelElement, String project, String modelId,
            Set<String> authColumns, List<String> dimensions, List<String> measures) {
        if (AclPermissionUtil.isAdmin()) {
            return;
        }
        aclEvaluate.checkProjectReadPermission(project);

        NDataModel model = getModelById(modelId, project);
        long jointCount = model.getJoinTables().stream()
                .filter(table -> authColumns
                        .containsAll(Arrays.stream(table.getJoin().getPrimaryKeyColumns())
                                .map(TblColRef::getAliasDotName).collect(Collectors.toSet()))
                        && authColumns.containsAll(Arrays.stream(table.getJoin().getForeignKeyColumns())
                                .map(TblColRef::getAliasDotName).collect(Collectors.toSet())))
                .count();
        long singleTableCount = model.getAllTables().stream().filter(ref -> ref.getColumns().stream()
                .map(TblColRef::getAliasDotName).collect(Collectors.toSet()).stream().anyMatch(authColumns::contains))
                .count();

        if (jointCount != model.getJoinTables().size() || singleTableCount == 0
                || (modelElement.equals(SyncContext.ModelElement.CUSTOM_COLS)
                        && !checkColumnPermission(model, authColumns, dimensions, measures))) {
            throw new KylinException(ServerErrorCode.INVALID_TABLE_AUTH,
                    MsgPicker.getMsg().getTableNoColumnsPermission());
        }
    }

    public boolean checkColumnPermission(NDataModel model, Set<String> authColumns, List<String> dimensions,
            List<String> measures) {

        if (!checkDimensionPermission(model, authColumns, dimensions)) {
            return false;
        }
        if (CollectionUtils.isEmpty(measures)) {
            return true;
        }
        List<MeasureDef> authMeasures = model.getEffectiveMeasures().values().stream()
                .filter(measure -> measures.contains(measure.getName()))
                .filter(measure -> checkMeasurePermission(authColumns, measure, model)).map(MeasureDef::new)
                .collect(Collectors.toList());
        return authMeasures.size() == measures.size();

    }

    private boolean checkDimensionPermission(NDataModel model, Set<String> authColumns, List<String> dimensions) {
        if (CollectionUtils.isEmpty(dimensions)) {
            return true;
        }
        List<ComputedColumnDesc> computedColumnDescs = model.getComputedColumnDescs().stream()
                .filter(cc -> dimensions.contains(cc.getFullName())).collect(Collectors.toList());

        long authComputedCount = computedColumnDescs.stream()
                .filter(cc -> authColumns.containsAll(convertCCToNormalCols(model, cc))).count();

        if (computedColumnDescs.size() != authComputedCount) {
            return false;
        }

        List<String> normalColumns = dimensions.stream().filter(column -> !computedColumnDescs.stream()
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toList()).contains(column))
                .collect(Collectors.toList());
        return authColumns.containsAll(normalColumns);
    }

    public Set<String> convertCCToNormalCols(NDataModel model, ComputedColumnDesc computedColumnDesc) {
        IndexDependencyParser parser = new IndexDependencyParser(model);
        try {
            Set<TblColRef> tblColRefList = parser.unwrapComputeColumn(computedColumnDesc.getInnerExpression());
            return tblColRefList.stream().map(TblColRef::getAliasDotName).collect(Collectors.toSet());
        } catch (Exception e) {
            log.warn("UnWrap computed column {} in project {} model {} exception",
                    computedColumnDesc.getInnerExpression(), model.getProject(), model.getAlias(), e);
        }
        return Collections.emptySet();
    }

    private boolean checkMeasurePermission(Set<String> authColumns, NDataModel.Measure measure, NDataModel model) {
        Set<String> measureColumns = measure.getFunction().getParameters().stream()
                .filter(parameterDesc -> parameterDesc.getColRef() != null)
                .map(parameterDesc -> parameterDesc.getColRef().getAliasDotName()).collect(Collectors.toSet());

        List<ComputedColumnDesc> computedColumnDescs = model.getComputedColumnDescs().stream()
                .filter(cc -> measureColumns.contains(cc.getFullName())).collect(Collectors.toList());

        long authComputedCount = computedColumnDescs.stream()
                .filter(cc -> authColumns.containsAll(convertCCToNormalCols(model, cc))).count();

        if (computedColumnDescs.size() != authComputedCount) {
            return false;
        }

        List<String> normalColumns = measureColumns.stream().filter(column -> !computedColumnDescs.stream()
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toList()).contains(column))
                .collect(Collectors.toList());

        return authColumns.containsAll(normalColumns);
    }

    private void checkModelExportPermission(String project, String modeId) {
        if (AclPermissionUtil.isAdmin()) {
            return;
        }
        aclEvaluate.checkProjectReadPermission(project);

        NDataModel model = getManager(NDataModelManager.class, project).getDataModelDesc(modeId);
        Map<String, Set<String>> modelTableColumns = new HashMap<>();
        for (TableRef tableRef : model.getAllTables()) {
            modelTableColumns.putIfAbsent(tableRef.getTableIdentity(), new HashSet<>());
            modelTableColumns.get(tableRef.getTableIdentity())
                    .addAll(tableRef.getColumns().stream().map(TblColRef::getName).collect(Collectors.toSet()));
        }
        AclTCRManager aclManager = AclTCRManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        String currentUserName = AclPermissionUtil.getCurrentUsername();
        Set<String> groupsOfExecuteUser = accessService.getGroupsOfExecuteUser(currentUserName);
        MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
        Set<String> groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfExecuteUser, acl);
        if (AclPermissionUtil.isAdminInProject(project, groupsOfExecuteUser)) {
            return;
        }
        AclTCRDigest digest = aclManager.getAllUnauthorizedTableColumn(currentUserName, groupsInProject,
                modelTableColumns);
        Set<String> authorizedCC = ComputedColumnUtil
                .getAuthorizedCC(Collections.singletonList(model),
                        ccSourceCols -> aclManager.isColumnsAuthorized(currentUserName, groupsOfExecuteUser,
                                ccSourceCols))
                .stream().map(ComputedColumnDesc::getIdentName).collect(Collectors.toSet());
        if (digest.getColumns() != null && !digest.getColumns().isEmpty()
                && digest.getColumns().stream().anyMatch(column -> !authorizedCC.contains(column))) {
            throw new KylinException(UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
        if (digest.getTables() != null && !digest.getTables().isEmpty()) {
            throw new KylinException(UNAUTHORIZED_ENTITY,
                    "current user does not have full permission on requesting model");
        }
    }
}
