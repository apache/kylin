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
import static org.apache.kylin.common.exception.ServerErrorCode.UNSUPPORTED_STREAMING_OPERATION;
import static org.apache.kylin.rest.util.TableUtils.calculateTableSize;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.controller.NBasicController;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.service.TableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = "/api/tables", produces = { HTTP_VND_APACHE_KYLIN_V2_JSON })
public class NTableControllerV2 extends NBasicController {

    @Autowired
    private TableService tableService;

    @ApiOperation(value = "getTableDesc", tags = { "AI" })
    @GetMapping(value = "")
    @ResponseBody
    public EnvelopeResponse<List<TableDesc>> getTableDesc(@RequestParam(value = "project") String project,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "database", required = false) String database,
            @RequestParam(value = "is_fuzzy", required = false, defaultValue = "false") boolean isFuzzy,
            @RequestParam(value = "ext", required = false, defaultValue = "true") boolean withExt,
            @RequestParam(value = "page_offset", required = false, defaultValue = "0") Integer offset,
            @RequestParam(value = "page_size", required = false, defaultValue = "10") Integer limit,
            @RequestParam(value = "source_type", required = false, defaultValue = "9") Integer sourceType)
            throws IOException {
        checkProjectName(project);
        checkNonNegativeIntegerArg("page_offset", offset);
        if (sourceType == ISourceAware.ID_STREAMING) {
            throw new KylinException(UNSUPPORTED_STREAMING_OPERATION,
                    MsgPicker.getMsg().getStreamingOperationNotSupport());
        }
        int returnTableSize = calculateTableSize(offset, limit);
        Pair<List<TableDesc>, Integer> tableDescWithActualSize = tableService.getTableDesc(project, withExt, table, database,
                isFuzzy, Collections.singletonList(sourceType), returnTableSize);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS,
                DataResult.getCustom(tableDescWithActualSize, offset, limit).getValue(),
                "");
    }
}