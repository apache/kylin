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

package org.apache.kylin.stream.server.rest.security;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.relnode.OLAPTableScan;
import org.apache.kylin.query.security.QueryInterceptor;

import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class StreamTableInterceptor extends QueryInterceptor {

    @Override
    protected boolean isEnabled() {
        return KylinConfig.getInstanceFromEnv().isTableACLEnabled();
    }

    @Override
    public Set<String> getQueryIdentifiers(List<OLAPContext> contexts) {
        return getAllTblsWithSchema(contexts);
    }

    @Override
    protected Set<String> getIdentifierBlackList(List<OLAPContext> contexts) {
        return Sets.newHashSet();
    }

    @Override
    protected String getIdentifierType() {
        return "table";
    }

    private Set<String> getAllTblsWithSchema(List<OLAPContext> contexts) {
        // all tables with DB, Like DB.TABLE
        Set<String> tableWithSchema = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (OLAPContext context : contexts) {
            for (OLAPTableScan tableScan : context.allTableScans) {
                tableWithSchema.add(tableScan.getTableRef().getTableIdentity());
            }
        }
        return tableWithSchema;
    }
}
