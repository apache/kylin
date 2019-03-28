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

package org.apache.kylin.stream.server.rest.controller;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.stream.server.rest.exception.InternalErrorException;
import org.apache.kylin.stream.server.rest.model.SQLRequest;
import org.apache.kylin.stream.server.rest.model.SQLResponse;
import org.apache.kylin.stream.server.rest.service.QueryService;
import org.apache.kylin.stream.server.rest.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Locale;

/**
 * Handle query requests.
 *
 */
@Controller
public class QueryController extends BasicController {

    public static final String SUCCESS_QUERY_CACHE = "StorageCache";
    public static final String EXCEPTION_QUERY_CACHE = "ExceptionQueryCache";
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);
    @Autowired
    private QueryService queryService;

    @RequestMapping(value = "/query", method = RequestMethod.POST, produces = { "application/json" })
    @ResponseBody
    public SQLResponse query(@RequestBody SQLRequest sqlRequest) {
        return doQueryWithCache(sqlRequest);
    }

    private SQLResponse doQueryWithCache(SQLRequest sqlRequest) {
        try {
            BackdoorToggles.setToggles(sqlRequest.getBackdoorToggles());

            String sql = sqlRequest.getSql();
            String project = sqlRequest.getProject();
            logger.info("Using project: " + project);
            logger.info("The original query:  " + sql);

            if (!sql.toLowerCase(Locale.ROOT).contains("select")) {
                logger.debug("Directly return exception as not supported");
                throw new InternalErrorException("Not Supported SQL.");
            }

            long startTime = System.currentTimeMillis();
            SQLResponse sqlResponse;
            try {
                sqlResponse = queryService.query(sqlRequest);
                sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                logger.info(
                        "Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                        new String[] { String.valueOf(sqlResponse.getIsException()),
                                String.valueOf(sqlResponse.getDuration()),
                                String.valueOf(sqlResponse.getTotalScanCount()) });

            } catch (Throwable e) { // calcite may throw AssertError
                logger.error("Exception when execute sql", e);
                String errMsg = QueryUtil.makeErrorMsgUserFriendly(e);

                sqlResponse = new SQLResponse(null, null, 0, true, errMsg);

            }

            if (sqlResponse.getIsException())
                throw new InternalErrorException(sqlResponse.getExceptionMessage());

            return sqlResponse;

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

}
