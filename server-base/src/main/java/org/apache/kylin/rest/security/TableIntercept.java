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

package org.apache.kylin.rest.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.query.security.QueryIntercept;
import org.apache.kylin.query.security.QueryInterceptUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableIntercept implements QueryIntercept{
    private static final Logger logger = LoggerFactory.getLogger(QueryIntercept.class);

    @Override
    public void intercept(String project, String username, List<OLAPContext> contexts) {
        List<String> userTableBlackList = getTableBlackList(project, username);
        if (userTableBlackList.isEmpty()) {
            return;
        }
        Set<String> queryTbls = getQueryIdentifiers(contexts);
        for (String tbl : userTableBlackList) {
            if (queryTbls.contains(tbl.toUpperCase())) {
                throw new AccessDeniedException("table:" + tbl);
            }
        }
    }

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        return QueryInterceptUtil.getAllTblsWithSchema(contexts);
    }

    private List<String> getTableBlackList(String project, String username) {
        List<String> tableBlackList = new ArrayList<>();
        try {
            tableBlackList = TableACLManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableACL(project).getTableBlackList(username);
        } catch (IOException e) {
            logger.error("get table black list fail. " + e.getMessage());
        }
        return tableBlackList;
    }
}
