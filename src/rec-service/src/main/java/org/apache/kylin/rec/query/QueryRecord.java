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

package org.apache.kylin.rec.query;

import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;

import lombok.Getter;

@Getter
public class QueryRecord {

    private final SQLResult sqlResult = new SQLResult();
    private final List<OlapContext> olapContexts = Lists.newArrayList();

    public void noteNonQueryException(String project, String sql, long elapsed) {
        sqlResult.writeNonQueryException(project, sql, elapsed);
    }

    public void noteException(String message, Throwable e) {
        sqlResult.writeExceptionInfo(message, e);
    }

    public void noteNormal(String project, String sql, long elapsed, String queryId) {
        sqlResult.writeNormalInfo(project, sql, elapsed, queryId);
    }

    public void noteOlapContexts() {
        ContextUtil.getThreadLocalContexts().forEach(ctx -> {
            ctx.setSql(sqlResult.getSql());
            ctx.simplify();
        });
        olapContexts.addAll(ContextUtil.getThreadLocalContexts());
    }
}
