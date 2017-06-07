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

package org.apache.kylin.rest.controller2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStore.Checkpoint;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.DataModelDescResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CacheService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.collect.Sets;

/**
 * ModelController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/models")
public class ModelControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ModelControllerV2.class);

    public static final char[] VALID_MODELNAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
            .toCharArray();

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("cacheService")
    private CacheService cacheService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getModelsPaging(@RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {
        HashMap<String, Object> data = new HashMap<String, Object>();
        List<DataModelDesc> models = modelService.listAllModels(modelName, projectName);

        List<DataModelDescResponse> dataModelDescResponses = new ArrayList<DataModelDescResponse>();
        for (DataModelDesc model : models) {
            DataModelDescResponse dataModelDescResponse = new DataModelDescResponse(model);
            if (model.isDraft()) {
                String parentName = model.getName().substring(0, model.getName().lastIndexOf("_draft"));
                DataModelDesc official = modelService.getMetadataManager().getDataModelDesc(parentName);
                if (official == null) {
                    dataModelDescResponse.setName(parentName);
                } else {
                    continue;
                }
            }
            if (projectName != null)
                dataModelDescResponse.setProject(projectName);
            else
                dataModelDescResponse.setProject(projectService.getProjectOfModel(model.getName()));
            dataModelDescResponses.add(dataModelDescResponse);
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;
        int size = dataModelDescResponses.size();

        if (size <= offset) {
            offset = size;
            limit = 0;
        }

        if ((size - offset) < limit) {
            limit = size - offset;
        }
        data.put("models", dataModelDescResponses.subList(offset, offset + limit));
        data.put("size", size);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDescV2(@RequestBody ModelRequest modelRequest) throws IOException {

        DataModelDesc modelDesc = deserializeDataModelDescV2(modelRequest);
        modelService.validateModelDesc(modelDesc);

        String projectName = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : modelRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        Checkpoint cp = store.checkpoint();
        try {
            boolean createNew = modelService.unifyModelDesc(modelDesc, false);
            modelDesc = modelService.updateModelToResourceStore(modelDesc, projectName, createNew, false);
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", modelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/draft", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateModelDescDraftV2(@RequestBody ModelRequest modelRequest) throws IOException {

        DataModelDesc modelDesc = deserializeDataModelDescV2(modelRequest);
        modelService.validateModelDesc(modelDesc);

        String projectName = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : modelRequest.getProject();

        ResourceStore store = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        Checkpoint cp = store.checkpoint();
        try {
            boolean createNew = modelService.unifyModelDesc(modelDesc, true);
            modelDesc = modelService.updateModelToResourceStore(modelDesc, projectName, createNew, true);
        } catch (Exception ex) {
            cp.rollback();
            cacheService.wipeAllCache();
            throw ex;
        } finally {
            cp.close();
        }

        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", modelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    @RequestMapping(value = "/{modelName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteModelV2(@PathVariable String modelName) throws IOException {
        Message msg = MsgPicker.getMsg();

        DataModelDesc desc = modelService.getMetadataManager().getDataModelDesc(modelName);
        if (null == desc) {
            throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), modelName));
        }
        modelService.dropModel(desc);
    }

    @RequestMapping(value = "/{modelName}/clone", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse cloneModelV2(@PathVariable String modelName, @RequestBody ModelRequest modelRequest)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        String project = modelRequest.getProject();
        MetadataManager metaManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc modelDesc = metaManager.getDataModelDesc(modelName);
        String newModelName = modelRequest.getModelName();

        if (StringUtils.isEmpty(project)) {
            logger.info("Project name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_PROJECT_NAME());
        }

        if (modelDesc == null || StringUtils.isEmpty(modelName)) {
            throw new BadRequestException(msg.getEMPTY_MODEL_NAME());
        }

        if (StringUtils.isEmpty(newModelName)) {
            logger.info("New model name is empty.");
            throw new BadRequestException(msg.getEMPTY_NEW_MODEL_NAME());
        }
        if (!StringUtils.containsOnly(newModelName, VALID_MODELNAME)) {
            logger.info("Invalid Model name {}, only letters, numbers and underline supported.", newModelName);
            throw new BadRequestException(String.format(msg.getINVALID_MODEL_NAME(), newModelName));
        }

        DataModelDesc newModelDesc = DataModelDesc.getCopyOf(modelDesc);
        newModelDesc.setName(newModelName);

        newModelDesc = modelService.createModelDesc(project, newModelDesc);

        //reload avoid shallow
        metaManager.reloadDataModelDescAt(DataModelDesc.concatResourcePath(newModelName));

        String descData = JsonUtil.writeValueAsIndentString(newModelDesc);
        GeneralResponse data = new GeneralResponse();
        data.setProperty("uuid", newModelDesc.getUuid());
        data.setProperty("modelDescData", descData);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private DataModelDesc deserializeDataModelDescV2(ModelRequest modelRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        DataModelDesc desc = null;
        try {
            logger.debug("Saving MODEL " + modelRequest.getModelDescData());
            desc = JsonUtil.readValue(modelRequest.getModelDescData(), DataModelDesc.class);
        } catch (JsonParseException e) {
            logger.error("The data model definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data model definition is not valid.", e);
            throw new BadRequestException(msg.getINVALID_MODEL_DEFINITION());
        }
        return desc;
    }

    @RequestMapping(value = "/{modelName}/usedCols", method = RequestMethod.GET, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getUsedColsV2(@PathVariable String modelName) {

        Map<String, Set<String>> data = new HashMap<>();

        for (Map.Entry<TblColRef, Set<CubeInstance>> entry : modelService.getUsedDimCols(modelName).entrySet()) {
            populateUsedColResponse(entry.getKey(), entry.getValue(), data);
        }

        for (Map.Entry<TblColRef, Set<CubeInstance>> entry : modelService.getUsedNonDimCols(modelName).entrySet()) {
            populateUsedColResponse(entry.getKey(), entry.getValue(), data);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private void populateUsedColResponse(TblColRef tblColRef, Set<CubeInstance> cubeInstances,
            Map<String, Set<String>> ret) {
        String columnIdentity = tblColRef.getIdentity();
        if (!ret.containsKey(columnIdentity)) {
            ret.put(columnIdentity, Sets.<String> newHashSet());
        }

        for (CubeInstance cubeInstance : cubeInstances) {
            ret.get(columnIdentity).add(cubeInstance.getCanonicalName());
        }
    }

    public void setModelService(ModelService modelService) {
        this.modelService = modelService;
    }

}
