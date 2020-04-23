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

package org.apache.kylin.query.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.calcite.sql.parser.SqlParseException;

import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.exceptions.KylinTimeoutException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.adhoc.PushDownRunnerJdbcImpl;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.source.adhocquery.IPushDownRunner;

import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Execute a pushdown query using a single or multiple runners depending on the configuration.
 */
public class PushDownExecutor {
    private static final Logger logger = LoggerFactory.getLogger(PushDownExecutor.class);
    private KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

    public PushDownExecutor() {

    }

    public Pair<List<List<String>>, List<SelectedColumnMeta>> pushDownQuery(String project, String sql,
            String defaultSchema, SQLException sqlException, boolean isSelect, boolean isPrepare) throws Exception {

        if (!kylinConfig.isPushDownEnabled()) {
            return null;
        }

        if (isSelect) {
            logger.info("Query failed to utilize pre-calculation, routing to other engines", sqlException);
            if (!isExpectedCause(sqlException)) {
                logger.info("quit doPushDownQuery because prior exception thrown is unexpected");
                return null;
            }
        } else {
            Preconditions.checkState(sqlException == null);
            logger.info("Kylin cannot support non-select queries, routing to other engines");
        }

        List<String> ids = kylinConfig.getPushDownRunnerIds();

        if (ids.isEmpty() && StringUtils.isNotEmpty(kylinConfig.getPushDownRunnerClassName())) {
            IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
            runner.init(kylinConfig);
            return queryBySingleRunner(runner, project, sql, defaultSchema, sqlException, isSelect, isPrepare);
        } else {
            return queryByMultiJdbcRunners(ids, project, sql, defaultSchema, sqlException, isSelect, isPrepare);
        }
    }

    private static boolean isExpectedCause(SQLException sqlException) {
        Preconditions.checkArgument(sqlException != null);
        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);

        //SqlValidatorException is not an excepted exception in the origin design.But in the multi pass scene,
        //query pushdown may create tables, and the tables are not in the model, so will throw SqlValidatorException.
        boolean isPushDownUpdateEnabled = KylinConfig.getInstanceFromEnv().isPushDownUpdateEnabled();

        if (isPushDownUpdateEnabled) {
            return (rootCause instanceof NoRealizationFoundException //
                    || rootCause instanceof RoutingIndicatorException || rootCause instanceof SqlValidatorException); //
        } else {
            if (rootCause instanceof KylinTimeoutException)
                return false;
            if (rootCause instanceof AccessDeniedException) {
                return false;
            }
            if (rootCause instanceof RoutingIndicatorException) {
                return true;
            }

            if (rootCause instanceof CompileException) {
                return true;
            }

            if (QueryContextFacade.current().isWithoutSyntaxError()) {
                logger.warn("route to push down for met error when running current query", sqlException);
                return true;
            }
        }

        return false;
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> queryBySingleRunner(IPushDownRunner runner,
            String project, String sql, String defaultSchema, SQLException sqlException, boolean isSelect,
            boolean isPrepare) throws Exception {

        logger.debug("Query Pushdown runner {}", runner);

        // default schema in calcite does not apply to other engines.
        // since this is a universql requirement, it's not implemented as a converter
        if (defaultSchema != null && !defaultSchema.equals("DEFAULT")) {
            String completed = sql;
            try {
                completed = PushDownUtil.schemaCompletion(sql, defaultSchema);
            } catch (SqlParseException e) {
                // fail to parse the pushdown sql, ignore
                logger.debug("fail to do schema completion on the pushdown sql, ignore it.", e.getMessage());
            }
            if (!sql.equals(completed)) {
                logger.info("the query is converted to {} after schema completion", completed);
                sql = completed;
            }
        }

        sql = runner.convertSql(kylinConfig, sql, project, defaultSchema, isPrepare);

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        if (isSelect) {
            runner.executeQuery(sql, returnRows, returnColumnMeta);
        }
        if (!isSelect && !isPrepare && kylinConfig.isPushDownUpdateEnabled()) {
            runner.executeUpdate(sql);
        }
        return Pair.newPair(returnRows, returnColumnMeta);
    }

    private Pair<List<List<String>>, List<SelectedColumnMeta>> queryByMultiJdbcRunners(List<String> ids, String project,
            String sql, String defaultSchema, SQLException sqlException, boolean isSelect, boolean isPrepare)
            throws Exception {
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            PushDownRunnerJdbcImpl runner = new PushDownRunnerJdbcImpl();
            runner.initById(kylinConfig, id);

            try {
                Pair<List<List<String>>, List<SelectedColumnMeta>> ret = queryBySingleRunner(runner, project, sql,
                        defaultSchema, sqlException, isSelect, isPrepare);
                if (null != ret) {
                    return ret;
                }
            } catch (Exception e) {
                logger.error("Execute pushdown query/update by jdbc runner " + id + " failed: "
                        + ExceptionUtils.getStackTrace(e));
            }
        }

        throw new RuntimeException("Execute pushdown query/update by multi jdbc runners failed");
    }
}