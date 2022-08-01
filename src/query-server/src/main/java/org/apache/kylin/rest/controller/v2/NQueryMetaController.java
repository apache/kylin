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

package org.apache.kylin.rest.controller.v2;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V2_JSON;

import java.util.List;

import org.apache.kylin.metadata.querymeta.TableMeta;
import org.apache.kylin.rest.request.MetaRequest;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.controller.NBasicController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.ApiOperation;

/**
 * Backward capable API for KyligenceODBC: /kylin/api/tables_and_columns
 *
 * Ref(KE 3.x): org.apache.kylin.rest.controller.QueryController.getMetadata(MetaRequest)
 *
 * TODO ODBC should support Newten API: /kylin/api/query/tables_and_columns
 *
 * @author yifanzhang
 *
 */
@RestController
@RequestMapping(value = "/api")
@Deprecated
public class NQueryMetaController extends NBasicController {

    @Autowired
    private QueryService queryService;

    @ApiOperation(value = "getMetadataForDriver", tags = { "QE" })
    @GetMapping(value = "/tables_and_columns", produces = { "application/json", HTTP_VND_APACHE_KYLIN_V2_JSON })
    @ResponseBody
    @Deprecated
    public List<TableMeta> getMetadataForDriver(MetaRequest metaRequest) {
        if (metaRequest.getModelAlias() == null) {
            return queryService.getMetadata(metaRequest.getProject());
        } else {
            return queryService.getMetadata(metaRequest.getProject(), metaRequest.getModelAlias());
        }
    }
}
