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


import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.security.AclPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiazhong
 */
@Component("modelMgmtService")
public class ModelService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(ModelService.class);

    @Autowired
    private AccessService accessService;

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<DataModelDesc> listAllModels(final String modelName, final String projectName) throws IOException{
        List<DataModelDesc> models = null;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            models = getMetadataManager().getModels();
        } else {
            models = getMetadataManager().getModels(projectName);
            project.getModels();
        }

        List<DataModelDesc> filterModels = new ArrayList();
        for (DataModelDesc modelDesc : models) {
            boolean isModelMatch = (null == modelName) || modelDesc.getName().toLowerCase().contains(modelName.toLowerCase());

            if (isModelMatch) {
                filterModels.add(modelDesc);
            }
        }

        return filterModels;
    }

    public List<DataModelDesc> getModels(final String modelName, final String projectName, final Integer limit, final Integer offset) throws IOException{

        List<DataModelDesc> modelDescs;
        modelDescs = listAllModels(modelName, projectName);

        if(limit==null || offset == null){
            return modelDescs;
        }

        if ((modelDescs.size() - offset) < limit) {
            return modelDescs.subList(offset, modelDescs.size());
        }

        return modelDescs.subList(offset, offset + limit);
    }


    public DataModelDesc createModelDesc(String projectName, DataModelDesc desc) throws IOException {
        if (getMetadataManager().getDataModelDesc(desc.getName()) != null) {
            throw new InternalErrorException("The model named " + desc.getName() + " already exists");
        }
        DataModelDesc createdDesc = null;
        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        createdDesc = getMetadataManager().createDataModelDesc(desc,projectName,owner);

        accessService.init(createdDesc, AclPermission.ADMINISTRATION);
        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdDesc, project);
        return createdDesc;
    }


    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public DataModelDesc updateModelAndDesc(DataModelDesc desc) throws IOException {

        getMetadataManager().updateDataModelDesc(desc);
        return desc;
    }


    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#desc, 'ADMINISTRATION') or hasPermission(#desc, 'MANAGEMENT')")
    public void dropModel(DataModelDesc desc) throws IOException {

        //check cube desc exist
        List<CubeDesc>  cubeDescs = getCubeDescManager().listAllDesc();
        for(CubeDesc cubeDesc:cubeDescs){
            if(cubeDesc.getModelName().equals(desc.getName())){
                throw new InternalErrorException("Model referenced by cube,drop cubes under model and try again.");
            }
        }

        //check II desc exist
        List<IIDesc> iiDescs = getIIDescManager().listAllDesc();
        for(IIDesc iidesc:iiDescs){
            if(iidesc.getModelName().equals(desc.getName())){
                throw new InternalErrorException("Model referenced by IIDesc.");
            }
        }

        getMetadataManager().dropModel(desc);

        accessService.clean(desc, true);
    }
}
