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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.service.ModelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * ModelController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/models")
public class ModelController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(ModelController.class);

    @Autowired
    private ModelService modelService;

    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<DataModelDesc> getModels(@RequestParam(value = "modelName", required = false) String modelName, @RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            return modelService.getModels(modelName, projectName, limit, offset);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    /**
     *
     * create model
     * @throws java.io.IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody
    public ModelRequest saveModelDesc(@RequestBody ModelRequest modelRequest) {
        //Update Model
        DataModelDesc modelDesc = deserializeDataModelDesc(modelRequest);
        if (modelDesc == null || StringUtils.isEmpty(modelDesc.getName())) {
            return modelRequest;
        }
        if (StringUtils.isEmpty(modelDesc.getName())) {
            logger.info("Model name should not be empty.");
            throw new BadRequestException("Model name should not be empty.");
        }

        try {
            modelDesc.setUuid(UUID.randomUUID().toString());
            String projectName = (null == modelRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : modelRequest.getProject();

            modelService.createModelDesc(projectName, modelDesc);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        modelRequest.setUuid(modelDesc.getUuid());
        modelRequest.setSuccessful(true);
        return modelRequest;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
    public ModelRequest updateModelDesc(@RequestBody ModelRequest modelRequest) throws JsonProcessingException {
        DataModelDesc modelDesc = deserializeDataModelDesc(modelRequest);
        if (modelDesc == null) {
            return modelRequest;
        }
        try {
            modelDesc = modelService.updateModelAndDesc(modelDesc);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this model.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        if (modelDesc.getError().isEmpty()) {
            modelRequest.setSuccessful(true);
        } else {
            logger.warn("Model " + modelDesc.getName() + " fail to update because " + modelDesc.getError());
            updateRequest(modelRequest, false, omitMessage(modelDesc.getError()));
        }
        String descData = JsonUtil.writeValueAsIndentString(modelDesc);
        modelRequest.setModelDescData(descData);
        return modelRequest;
    }

    @RequestMapping(value = "/{modelName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteModel(@PathVariable String modelName) {
        DataModelDesc desc = modelService.getMetadataManager().getDataModelDesc(modelName);
        if (null == desc) {
            throw new NotFoundException("Data Model with name " + modelName + " not found..");
        }
        try {
            modelService.dropModel(desc);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete model. " + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{modelName}/clone", method = {RequestMethod.PUT})
    @ResponseBody
    public ModelRequest  cloneModel(@PathVariable String modelName, @RequestBody ModelRequest modelRequest) {
        String project = modelRequest.getProject();
        MetadataManager metaManager = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        DataModelDesc modelDesc = metaManager.getDataModelDesc(modelName);
        String newModelName = modelRequest.getModelName();

        if (StringUtils.isEmpty(project)) {
            logger.info("Project name should not be empty.");
            throw new BadRequestException("Project name should not be empty.");
        }

        if (modelDesc == null || StringUtils.isEmpty(modelName)) {
            logger.info("Model does not exist.");
            throw new BadRequestException("Model does not exist.");
        }

        if (StringUtils.isEmpty(newModelName)) {
            logger.info("New model name is empty.");
            throw new BadRequestException("New model name is empty.");
        }


        DataModelDesc newModelDesc = DataModelDesc.getCopyOf(modelDesc);
        newModelDesc.setName(newModelName);
        try {
            newModelDesc = modelService.createModelDesc(project, newModelDesc);

            //reload avoid shallow
            metaManager.reloadDataModelDesc(newModelName);
        } catch (IOException e) {
            throw new InternalErrorException("failed to clone DataModelDesc", e);
        }

        modelRequest.setUuid(newModelDesc.getUuid());
        modelRequest.setSuccessful(true);
        return modelRequest;
    }

    private DataModelDesc deserializeDataModelDesc(ModelRequest modelRequest) {
        DataModelDesc desc = null;
        try {
            logger.debug("Saving MODEL " + modelRequest.getModelDescData());
            desc = JsonUtil.readValue(modelRequest.getModelDescData(), DataModelDesc.class);
        } catch (JsonParseException e) {
            logger.error("The data model definition is not valid.", e);
            updateRequest(modelRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data model definition is not valid.", e);
            updateRequest(modelRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private void updateRequest(ModelRequest request, boolean success, String message) {
        request.setModelDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setModelService(ModelService modelService) {
        this.modelService = modelService;
    }

    /**
     * @param errors
     * @return
     */
    private String omitMessage(List<String> errors) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator<String> iterator = errors.iterator(); iterator.hasNext();) {
            String string = (String) iterator.next();
            buffer.append(string);
            buffer.append("\n");
        }
        return buffer.toString();
    }
}
