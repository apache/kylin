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

package org.apache.kylin.rec.query.mockup;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.guava30.shaded.common.base.Stopwatch;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rec.query.QueryRecord;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockupQueryExecutor extends AbstractQueryExecutor {

    @Override
    public QueryRecord execute(String project, KylinConfig kylinConfig, String sql) {
        Stopwatch watch = Stopwatch.createStarted();
        ContextUtil.clearThreadLocalContexts();
        QueryContext.reset();
        // set to check all models, rather than skip models when finding a realization
        // in RealizationChooser#attemptSelectRealization
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_CHECK_ALL_MODELS, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DISABLE_RAW_QUERY_HACKER, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");

        QueryRecord record = getCurrentRecord();
        QueryContext.current().setForModeling(true);
        if (!QueryUtil.isSelectStatement(sql)) {
            record.noteNonQueryException(project, sql, watch.elapsed(TimeUnit.MILLISECONDS));
            return record;
        }

        try {
            // execute and discard the result data
            QueryExec queryExec = new QueryExec(project, kylinConfig);
            QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, queryExec.getDefaultSchemaName(),
                    true);
            queryExec.executeQuery(QueryUtil.massageSql(queryParams));
        } catch (Throwable e) { // cannot replace with Exception, e may a instance of Error
            Throwable cause = e.getCause();
            String message = e.getMessage() == null
                    ? String.format(Locale.ROOT, "%s, check kylin.log for details", e.getClass().toString())
                    : QueryUtil.makeErrorMsgUserFriendly(e);
            if (!(cause instanceof NoRealizationFoundException)) {
                log.debug("Failed to run in MockupQueryExecutor. Critical stackTrace:\n{}",
                        ThreadUtil.getKylinStackTrace());
            }
            record.noteException(message, e);
        } finally {
            record.noteNormal(project, sql, watch.elapsed(TimeUnit.MILLISECONDS), QueryContext.current().getQueryId());
            record.noteOlapContexts();
            record.getOlapContexts().forEach(ctx -> ctx.setAggregations(transformSpecialFunctions(ctx)));
            clearCurrentRecord();
        }

        return record;
    }

    private List<FunctionDesc> transformSpecialFunctions(OlapContext ctx) {
        return ctx.getAggregations().stream().map(func -> {
            if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                ctx.getGroupByColumns().add(func.getParameters().get(1).getColRef());
                return FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT, func.getParameters().subList(0, 1),
                        "bitmap");
            } else if (FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase((func.getExpression()))) {
                return FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT, func.getParameters().subList(0, 1),
                        "bitmap");
            } else {
                return func;
            }
        }).collect(Collectors.toList());
    }
}
