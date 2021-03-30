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

package org.apache.kylin.query.security;

import java.util.Collection;
import java.util.List;

import java.util.Locale;
import org.apache.kylin.query.relnode.OLAPContext;

public abstract class QueryInterceptor {

    public void intercept(List<OLAPContext> contexts) {
        if (!isEnabled()) {
            return;
        }
        Collection<String> userIdentifierBlackList = getIdentifierBlackList(contexts);
        intercept(contexts, userIdentifierBlackList);
    }

    private void intercept(List<OLAPContext> contexts, Collection<String> blackList) {
        if (blackList.isEmpty()) {
            return;
        }

        Collection<String> queryCols = getQueryIdentifiers(contexts);
        for (String id : blackList) {
            if (queryCols.contains(id.toUpperCase(Locale.ROOT))) {
                throw new AccessDeniedException(getIdentifierType() + ":" + id);
            }
        }
    }

    protected abstract boolean isEnabled();

    protected abstract Collection<String> getQueryIdentifiers(List<OLAPContext> contexts);

    protected abstract Collection<String> getIdentifierBlackList(List<OLAPContext> contexts);

    protected abstract String getIdentifierType();

    protected String getProject(List<OLAPContext> contexts) {
        return contexts.get(0).olapSchema.getProjectName();
    }

    protected String getUser(List<OLAPContext> contexts) {
        return contexts.get(0).olapAuthen.getUsername();
    }
}