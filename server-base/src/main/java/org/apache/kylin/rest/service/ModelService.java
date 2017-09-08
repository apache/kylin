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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author jiazhong
 */
@Component("modelMgmtService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    public static final char[] VALID_MODELNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    private AclEvaluate aclEvaluate;

    public List<DataModelDesc> listAllModels(final String modelName, final String projectName, boolean exactMatch)
            throws IOException {
        List<DataModelDesc> models;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            aclEvaluate.checkIsGlobalAdmin();
            models = getMetadataManager().getModels();
        } else {
            aclEvaluate.hasProjectReadPermission(project);
            models = getMetadataManager().getModels(projectName);
        }

        List<DataModelDesc> filterModels = new ArrayList<DataModelDesc>();
        for (DataModelDesc modelDesc : models) {
            boolean isModelMatch = (null == modelName) || modelName.length() == 0
                    || (exactMatch && modelDesc.getName().toLowerCase().equals(modelName.toLowerCase()))
                    || (!exactMatch && modelDesc.getName().toLowerCase().contains(modelName.toLowerCase()));

            if (isModelMatch) {
                filterModels.add(modelDesc);
            }
        }

        return filterModels;
    }

    public List<DataModelDesc> getModels(final String modelName, final String projectName, final Integer limit,
            final Integer offset) throws IOException {

        List<DataModelDesc> modelDescs = listAllModels(modelName, projectName, true);

        if (limit == null || offset == null) {
            return modelDescs;
        }

        if ((modelDescs.size() - offset) < limit) {
            return modelDescs.subList(offset, modelDescs.size());
        }

        return modelDescs.subList(offset, offset + limit);
    }

    public DataModelDesc createModelDesc(String projectName, DataModelDesc desc) throws IOException {
        aclEvaluate.hasProjectWritePermission(getProjectManager().getProject(projectName));
        Message msg = MsgPicker.getMsg();

        if (getMetadataManager().getDataModelDesc(desc.getName()) != null) {
            throw new BadRequestException(String.format(msg.getDUPLICATE_MODEL_NAME(), desc.getName()));
        }
        DataModelDesc createdDesc = null;
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        createdDesc = getMetadataManager().createDataModelDesc(desc, projectName, owner);

        if (!desc.isDraft()) {
            accessService.init(createdDesc, AclPermission.ADMINISTRATION);
            ProjectInstance project = getProjectManager().getProject(projectName);
            accessService.inherit(createdDesc, project);
        }
        return createdDesc;
    }

    public DataModelDesc updateModelAndDesc(String project, DataModelDesc desc) throws IOException {
        aclEvaluate.checkProjectWritePermission(project);
        getMetadataManager().updateDataModelDesc(desc);
        return desc;
    }

    public void dropModel(DataModelDesc desc) throws IOException {
        aclEvaluate.hasProjectWritePermission(desc.getProjectInstance());
        Message msg = MsgPicker.getMsg();
        //check cube desc exist
        List<CubeDesc> cubeDescs = getCubeDescManager().listAllDesc();
        for (CubeDesc cubeDesc : cubeDescs) {
            if (cubeDesc.getModelName().equals(desc.getName())) {
                throw new BadRequestException(String.format(msg.getDROP_REFERENCED_MODEL(), cubeDesc.getName()));
            }
        }

        getMetadataManager().dropModel(desc);

        accessService.clean(desc, true);
    }

    public boolean isTableInAnyModel(TableDesc table) {
        return getMetadataManager().isTableInAnyModel(table);
    }

    public boolean isTableInModel(TableDesc table, String project) throws IOException {
        return getMetadataManager().getModelsUsingTable(table, project).size() > 0;
    }

    public List<String> getModelsUsingTable(TableDesc table, String project) throws IOException {
        return getMetadataManager().getModelsUsingTable(table, project);
    }

    public Map<TblColRef, Set<CubeInstance>> getUsedDimCols(String modelName, String project) {
        Map<TblColRef, Set<CubeInstance>> ret = Maps.newHashMap();
        List<CubeInstance> cubeInstances = cubeService.listAllCubes(null, project, modelName, true);
        for (CubeInstance cubeInstance : cubeInstances) {
            CubeDesc cubeDesc = cubeInstance.getDescriptor();
            for (TblColRef tblColRef : cubeDesc.listDimensionColumnsIncludingDerived()) {
                if (ret.containsKey(tblColRef)) {
                    ret.get(tblColRef).add(cubeInstance);
                } else {
                    Set<CubeInstance> set = Sets.newHashSet(cubeInstance);
                    ret.put(tblColRef, set);
                }
            }
        }
        return ret;
    }

    public Map<TblColRef, Set<CubeInstance>> getUsedNonDimCols(String modelName, String project) {
        Map<TblColRef, Set<CubeInstance>> ret = Maps.newHashMap();
        List<CubeInstance> cubeInstances = cubeService.listAllCubes(null, project, modelName, true);
        for (CubeInstance cubeInstance : cubeInstances) {
            CubeDesc cubeDesc = cubeInstance.getDescriptor();
            Set<TblColRef> tblColRefs = Sets.newHashSet(cubeDesc.listAllColumns());//make a copy
            tblColRefs.removeAll(cubeDesc.listDimensionColumnsIncludingDerived());
            for (TblColRef tblColRef : tblColRefs) {
                if (ret.containsKey(tblColRef)) {
                    ret.get(tblColRef).add(cubeInstance);
                } else {
                    Set<CubeInstance> set = Sets.newHashSet(cubeInstance);
                    ret.put(tblColRef, set);
                }
            }
        }
        return ret;
    }

    /**
     * check if the computed column expressions are valid ( in hive)
     */
    public boolean checkCCExpression(final DataModelDesc dataModelDesc, String project) throws IOException {

        dataModelDesc.setDraft(false);
        if (dataModelDesc.getUuid() == null)
            dataModelDesc.updateRandomUuid();

        dataModelDesc.init(getConfig(), getMetadataManager().getAllTablesMap(project),
                getMetadataManager().listDataModels());

        for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {

            //check by calcite parser
            cc.simpleParserCheck(cc.getExpression(), dataModelDesc.getAliasMap().keySet());

            //check by hive cli, this could be slow
            StringBuilder sb = new StringBuilder();
            sb.append("select ");
            sb.append(cc.getExpression());
            sb.append(" ");
            JoinedFlatTable.appendJoinStatement(new IJoinedFlatTableDesc() {
                @Override
                public String getTableName() {
                    return null;
                }

                @Override
                public DataModelDesc getDataModel() {
                    return dataModelDesc;
                }

                @Override
                public List<TblColRef> getAllColumns() {
                    return null;
                }

                @Override
                public int getColumnIndex(TblColRef colRef) {
                    return 0;
                }

                @Override
                public SegmentRange getSegRange() {
                    return null;
                }

                @Override
                public TblColRef getDistributedBy() {
                    return null;
                }

                @Override
                public TblColRef getClusterBy() {
                    return null;
                }

                @Override
                public ISegment getSegment() {
                    return null;
                }
            }, sb, false);
            sb.append(" limit 0");

            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.addStatement(sb.toString());

            long ts = System.currentTimeMillis();
            Pair<Integer, String> response = KylinConfig.getInstanceFromEnv().getCliCommandExecutor()
                    .execute(hiveCmdBuilder.toString());
            logger.debug("Spent " + (System.currentTimeMillis() - ts)
                    + " ms to execute the hive command to validate computed column expression: " + cc.getExpression());
            if (response.getFirst() != 0) {
                throw new IllegalArgumentException("The expression " + cc.getExpression()
                        + " failed syntax check with output message: " + response.getSecond());
            }
        }

        return true;
    }

    private List<String> getModelCols(DataModelDesc model) {
        List<String> dimCols = new ArrayList<String>();

        List<ModelDimensionDesc> dimensions = model.getDimensions();

        for (ModelDimensionDesc dim : dimensions) {
            String table = dim.getTable();
            for (String c : dim.getColumns()) {
                dimCols.add(table + "." + c);
            }
        }
        return dimCols;
    }

    private List<String> getModelMeasures(DataModelDesc model) {
        List<String> measures = new ArrayList<String>();

        for (String s : model.getMetrics()) {
            measures.add(s);
        }
        return measures;
    }

    private Map<String, List<String>> getInfluencedCubesByDims(List<String> dims, List<CubeInstance> cubes) {
        Map<String, List<String>> influencedCubes = new HashMap<>();
        for (CubeInstance cubeInstance : cubes) {
            CubeDesc cubeDesc = cubeInstance.getDescriptor();
            for (TblColRef tblColRef : cubeDesc.listDimensionColumnsIncludingDerived()) {
                if (dims.contains(tblColRef.getIdentity()))
                    continue;
                if (influencedCubes.get(tblColRef.getIdentity()) == null) {
                    List<String> candidates = new ArrayList<>();
                    candidates.add(cubeInstance.getName());
                    influencedCubes.put(tblColRef.getIdentity(), candidates);
                } else
                    influencedCubes.get(tblColRef.getIdentity()).add(cubeInstance.getName());
            }
        }
        return influencedCubes;
    }

    private Map<String, List<String>> getInfluencedCubesByMeasures(List<String> allCols, List<CubeInstance> cubes) {
        Map<String, List<String>> influencedCubes = new HashMap<>();
        for (CubeInstance cubeInstance : cubes) {
            CubeDesc cubeDesc = cubeInstance.getDescriptor();
            Set<TblColRef> tblColRefs = Sets.newHashSet(cubeDesc.listAllColumns());
            tblColRefs.removeAll(cubeDesc.listDimensionColumnsIncludingDerived());
            for (TblColRef tblColRef : tblColRefs) {
                if (allCols.contains(tblColRef.getIdentity()))
                    continue;
                if (influencedCubes.get(tblColRef.getIdentity()) == null) {
                    List<String> candidates = new ArrayList<>();
                    candidates.add(cubeInstance.getName());
                    influencedCubes.put(tblColRef.getIdentity(), candidates);
                } else
                    influencedCubes.get(tblColRef.getIdentity()).add(cubeInstance.getName());
            }
        }
        return influencedCubes;
    }

    private String checkIfBreakExistingCubes(DataModelDesc dataModelDesc, String project) throws IOException {
        String modelName = dataModelDesc.getName();
        List<CubeInstance> cubes = cubeService.listAllCubes(null, project, modelName, true);
        DataModelDesc originDataModelDesc = listAllModels(modelName, project, true).get(0);

        StringBuilder checkRet = new StringBuilder();
        if (cubes != null && cubes.size() != 0) {
            dataModelDesc.init(getConfig(), getMetadataManager().getAllTablesMap(project),
                    getMetadataManager().listDataModels());

            List<String> curModelDims = getModelCols(dataModelDesc);
            List<String> curModelMeasures = getModelMeasures(dataModelDesc);

            List<String> curModelDimsAndMeasures = new ArrayList<>();
            curModelDimsAndMeasures.addAll(curModelDims);
            curModelDimsAndMeasures.addAll(curModelMeasures);

            Map<String, List<String>> influencedCubesByDims = getInfluencedCubesByDims(curModelDims, cubes);
            Map<String, List<String>> influencedCubesByMeasures = getInfluencedCubesByMeasures(curModelDimsAndMeasures,
                    cubes);

            for (Map.Entry<String, List<String>> e : influencedCubesByDims.entrySet()) {
                checkRet.append("Dimension: ");
                checkRet.append(e.getKey());
                checkRet.append(" can't be removed, It is referred in Cubes: ");
                checkRet.append(e.getValue().toString());
                checkRet.append("\r\n");
            }

            for (Map.Entry<String, List<String>> e : influencedCubesByMeasures.entrySet()) {
                checkRet.append("Measure: ");
                checkRet.append(e.getKey());
                checkRet.append(" can't be removed, It is referred in Cubes: ");
                checkRet.append(e.getValue().toString());
                checkRet.append("\r\n");
            }

            if (!dataModelDesc.getRootFactTable().equals(originDataModelDesc.getRootFactTable()))
                checkRet.append("Root fact table can't be modified. \r\n");

            JoinsTree joinsTree = dataModelDesc.getJoinsTree(), originJoinsTree = originDataModelDesc.getJoinsTree();
            if (joinsTree.matchNum(originJoinsTree) != originDataModelDesc.getJoinTables().length + 1)
                checkRet.append("The join shouldn't be modified in this model.");
        }
        return checkRet.toString();
    }

    public void primaryCheck(DataModelDesc modelDesc) {
        Message msg = MsgPicker.getMsg();

        if (modelDesc == null) {
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        }

        String modelName = modelDesc.getName();

        if (StringUtils.isEmpty(modelName)) {
            logger.info("Model name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(modelName, VALID_MODELNAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", modelDesc.getName());
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), modelName));
        }
    }

    public DataModelDesc updateModelToResourceStore(DataModelDesc modelDesc, String projectName) throws IOException {
        aclEvaluate.checkProjectWritePermission(projectName);
        Message msg = MsgPicker.getMsg();

        modelDesc.setDraft(false);
        if (modelDesc.getUuid() == null)
            modelDesc.updateRandomUuid();

        try {
            if (modelDesc.getLastModified() == 0) {
                // new
                modelDesc = createModelDesc(projectName, modelDesc);
            } else {
                // update
                String error = checkIfBreakExistingCubes(modelDesc, projectName);
                if (!error.isEmpty()) {
                    throw new BadRequestException(error);
                }
                modelDesc = updateModelAndDesc(projectName, modelDesc);
            }
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException(msg.getUPDATE_MODEL_NO_RIGHT());
        }

        if (!modelDesc.getError().isEmpty()) {
            throw new BadRequestException(String.format(msg.getBROKEN_MODEL_DESC(), modelDesc.getName()));
        }

        return modelDesc;
    }

    public DataModelDesc getModel(final String modelName, final String projectName) throws IOException {
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;
        if (null == project) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.hasProjectReadPermission(project);
        }

        return getMetadataManager().getDataModelDesc(modelName);
    }

    public Draft getModelDraft(String modelName, String projectName) throws IOException {
        for (Draft d : listModelDrafts(modelName, projectName)) {
            return d;
        }
        return null;
    }

    public List<Draft> listModelDrafts(String modelName, String projectName) throws IOException {
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;
        if (null == project) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.hasProjectReadPermission(project);
        }

        List<Draft> result = new ArrayList<>();

        for (Draft d : getDraftManager().list(projectName)) {
            RootPersistentEntity e = d.getEntity();
            if (e instanceof DataModelDesc) {
                DataModelDesc m = (DataModelDesc) e;
                if (modelName == null || modelName.equals(m.getName()))
                    result.add(d);
            }
        }

        return result;
    }
}
