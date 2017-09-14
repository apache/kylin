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

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.acl.TableACLManager;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.security.QueryIntercept;
import org.apache.kylin.query.security.QueryInterceptUtil;

public class TableIntercept extends QueryIntercept{

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        return QueryInterceptUtil.getAllTblsWithSchema(contexts);
    }

    @Override
    protected Set<String> getIdentifierBlackList(List<OLAPContext> contexts) {
        String project = getProject(contexts);
        String username = getUser(contexts);

        return TableACLManager
                .getInstance(KylinConfig.getInstanceFromEnv())
                .getTableACLByCache(project)
                .getTableBlackList(username);
    }

    @Override
    protected String getIdentifierType() {
        return "table";
    }
}
