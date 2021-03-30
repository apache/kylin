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
import java.util.List;
import java.util.Locale;

import org.apache.kylin.metadata.model.ExternalFilterDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component("extFilterService")
public class ExtFilterService extends BasicService {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ExtFilterService.class);

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void saveExternalFilter(ExternalFilterDesc desc) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (getTableManager().getExtFilterDesc(desc.getName()) != null) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getFILTER_ALREADY_EXIST(), desc.getName()));
        }
        getTableManager().saveExternalFilter(desc);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void updateExternalFilter(ExternalFilterDesc desc) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (getTableManager().getExtFilterDesc(desc.getName()) == null) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getFILTER_NOT_FOUND(), desc.getName()));
        }
        getTableManager().saveExternalFilter(desc);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void removeExternalFilter(String name) throws IOException {
        getTableManager().removeExternalFilter(name);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void syncExtFilterToProject(String[] filters, String project) throws IOException {
        getProjectManager().addExtFilterToProject(filters, project);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void removeExtFilterFromProject(String filterName, String projectName) throws IOException {
        getProjectManager().removeExtFilterFromProject(filterName, projectName);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public List<ExternalFilterDesc> listAllExternalFilters() {
        return getTableManager().listAllExternalFilters();
    }

}
