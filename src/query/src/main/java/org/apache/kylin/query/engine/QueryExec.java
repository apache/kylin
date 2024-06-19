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

package org.apache.kylin.query.engine;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.kylin.cache.kylin.KylinCacheFileSystem;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.ReadFsSwitch;
import org.apache.kylin.common.exception.DryRunSucceedException;
import org.apache.kylin.fileseg.FileSegments;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.StructField;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.engine.data.QueryResult;
import org.apache.kylin.query.engine.exec.CalcitePlanExec;
import org.apache.kylin.query.engine.exec.ExecuteResult;
import org.apache.kylin.query.engine.exec.SparderPlanExec;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.engine.view.ModelViewExpander;
import org.apache.kylin.query.engine.view.ViewAnalyzer;
import org.apache.kylin.query.mask.QueryResultMasks;
import org.apache.kylin.query.optrule.OlapFilterJoinRule;
import org.apache.kylin.query.optrule.OlapProjectJoinTransposeRule;
import org.apache.kylin.query.optrule.SumConstantConvertRule;
import org.apache.kylin.query.optrule.UnionTypeCastRule;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapAggregateRel;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.query.util.CalcitePlanRouterVisitor;
import org.apache.kylin.query.util.HepUtils;
import org.apache.kylin.query.util.QueryContextCutter;
import org.apache.kylin.query.util.QueryHelper;
import org.apache.kylin.query.util.QueryInterruptChecker;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.RelAggPushDownUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;

/**
 * Entrance for query execution
 */
public class QueryExec {
    private static final Logger logger = LoggerFactory.getLogger(QueryExec.class);

    private static final int MAX_TRY_TIMES_OPTIMIZED = 10;
    // only for test
    @Setter
    @Getter
    private String sparderQueryOptimizedExceptionMsg = "";

    private final KylinConfig kylinConfig;
    private final KylinConnectionConfig connectionConfig;
    private final RelOptPlanner planner;
    private final ProjectSchemaFactory schemaFactory;
    private final Prepare.CatalogReader catalogReader;
    private final SqlConverter sqlConverter;
    private final QueryOptimizer queryOptimizer;
    private final SimpleDataContext dataContext;
    private final boolean allowAlternativeQueryPlan;
    private final CalciteSchema rootSchema;
    private final String project;

    public QueryExec(String project, KylinConfig kylinConfig, boolean allowAlternativeQueryPlan) {
        this.project = project;
        this.kylinConfig = kylinConfig;
        connectionConfig = KylinConnectionConfig.fromKapConfig(kylinConfig);
        schemaFactory = new ProjectSchemaFactory(project, kylinConfig);
        rootSchema = schemaFactory.createProjectRootSchema();
        String defaultSchemaName = schemaFactory.getDefaultSchema();
        catalogReader = SqlConverter.createCatalogReader(connectionConfig, rootSchema, defaultSchemaName);
        planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(connectionConfig);
        sqlConverter = QueryExec.createConverter(connectionConfig, planner, catalogReader);
        dataContext = createDataContext(rootSchema);
        planner.setExecutor(new RexExecutorImpl(dataContext));
        queryOptimizer = new QueryOptimizer(planner);
        this.allowAlternativeQueryPlan = allowAlternativeQueryPlan;

        if (kylinConfig.getAutoModelViewEnabled()) {
            schemaFactory.addModelViewSchemas(rootSchema, new SimpleViewAnalyzer(rootSchema, defaultSchemaName));
        }
    }

    public class SimpleViewAnalyzer implements ViewAnalyzer {

        private final CalciteSchema rootSchema;
        private final String defaultSchemaName;

        public SimpleViewAnalyzer(CalciteSchema rootSchema, String defaultSchemaName) {
            this.rootSchema = rootSchema;
            this.defaultSchemaName = defaultSchemaName;
        }

        @Override
        public CalcitePrepare.AnalyzeViewResult analyzeView(String sql) throws SqlParseException {
            Prepare.CatalogReader viewCatalogReader = SqlConverter.createCatalogReader(connectionConfig, rootSchema,
                    defaultSchemaName);
            RelOptPlanner viewPlanner = new PlannerFactory(kylinConfig).createVolcanoPlanner(connectionConfig);
            SqlConverter viewSqlConverter = createConverter(connectionConfig, viewPlanner, viewCatalogReader);
            SimpleDataContext viewDataContext = createDataContext(rootSchema);
            viewPlanner.setExecutor(new RexExecutorImpl(viewDataContext));
            return viewSqlConverter.analyzeSQl(sql);
        }
    }

    public QueryExec(String project, KylinConfig kylinConfig) {
        this(project, kylinConfig, false);
    }

    public static SqlConverter createConverter(KylinConnectionConfig connectionConfig, RelOptPlanner planner,
            Prepare.CatalogReader catalogReader) {
        // this could be a bit awkward that SQLConverter and ViewExpander seem to have a cyclical reference
        SqlConverter sqlConverter = new SqlConverter(connectionConfig, planner, catalogReader);
        return new SqlConverter(connectionConfig, planner, catalogReader,
                new ModelViewExpander(sqlConverter::convertSqlToRelNode));
    }

    public void plannerRemoveRules(List<RelOptRule> rules) {
        for (RelOptRule rule : rules) {
            planner.removeRule(rule);
        }
    }

    public void plannerAddRules(List<RelOptRule> rules) {
        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }
    }

    /**
     * parse, optimize sql and execute the sql physically
     * @param sql
     * @return query result data with column infos
     */
    public QueryResult executeQuery(String sql) throws SQLException {
        magicDirts(sql);
        QueryContext queryContext = QueryContext.current();
        queryContext.setProject(project);
        try {
            if (kylinConfig.isQueryDryRunEnabled()) {
                logger.trace("Dry run Mode.");
                QueryContext.current().setDryRun(true);
            }
            beforeQuery();

            // process cache hint like -- select * from xxx /*+ ACCEPT_CACHE_TIME(158176387682000) */
            processAcceptCacheTime(queryContext.getFirstHintStr());

            QueryContext.currentTrace().startSpan(QueryTrace.SQL_PARSE_AND_OPTIMIZE);
            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
            queryContext.record("end_convert_to_relnode");
            RelNode node = queryOptimizer.optimize(relRoot).rel;
            queryContext.record("end_calcite_optimize");

            List<StructField> resultFields = RelColumnMetaDataExtractor.getColumnMetadata(relRoot.validatedRowType);
            if (resultFields.isEmpty()) {
                // result fields size may be 0 because of ACL controls and should return immediately
                QueryContext.fillEmptyResultSetMetrics();
                return new QueryResult();
            }

            if (kylinConfig.getEmptyResultForSelectStar() && QueryUtil.isSelectStarStatement(sql)
                    && !QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                return new QueryResult(Lists.newArrayList(), 0, resultFields);
            }

            List<String> columnNames = resultFields.stream().map(StructField::getName).collect(Collectors.toList());
            QueryContext.current().setColumnNames(columnNames);

            QueryResultMasks.setRootRelNode(node);
            QueryResult queryResult = new QueryResult(executeQueryPlan(postOptimize(node)), resultFields);
            if (queryContext.getQueryTagInfo().isAsyncQuery()) {
                AsyncQueryUtil.saveMetaDataAndFileInfo(queryContext, queryResult.getColumnMetas());
            }
            return queryResult;
        } catch (SqlParseException e) {
            // some special message for parsing error... to be compatible with avatica's error msg
            throw newSqlException(sql, "parse failed: " + e.getMessage(), e);
        } catch (Exception e) {
            // retry query if switched to backup read FS
            if (ReadFsSwitch.turnOnSwitcherIfBackupFsAllowed(e,
                    KapConfig.wrap(kylinConfig).getSwitchBackupFsExceptionAllowString())) {
                logger.info("Retry sql after hitting allowed exception and turn on backup read FS", e);
                return executeQuery(sql);
            }
            throw newSqlException(sql, e.getMessage(), e);
        } finally {
            afterQuery();
        }
    }

    public List<OlapContext> deriveOlapContexts(String sql) {
        List<OlapContext> contexts = Lists.newArrayList();
        try {
            ContextUtil.clearThreadLocalContexts();
            RelNode relNode = parseAndOptimize(sql);
            QueryContextCutter.analyzeOlapContext(relNode);
            Collection<OlapContext> tmp = ContextUtil.getThreadLocalContexts();
            contexts.addAll(tmp);
        } catch (Exception e) {
            logger.error("Sql Parsing error.", e);
        } finally {
            ContextUtil.clearThreadLocalContexts();
        }
        return contexts;
    }

    private void magicDirts(String sql) {
        if (sql.contains("ReadFsSwitch.turnOnBackupFsWhile")) {
            ReadFsSwitch.turnOnBackupFsWhile();
        }
    }

    /**
     * Separating <code>parseAndOptimize</code> and <code>postOptimize</code> is only for UT test.
     */
    @VisibleForTesting
    public RelNode parseAndOptimize(String sql) throws SqlParseException {
        beforeQuery();
        RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);
        return queryOptimizer.optimize(relRoot).rel;
    }

    /**
     * Apply post optimization rules and produce a list of alternative transformed nodes
     */
    public List<RelNode> postOptimize(RelNode node) {
        Collection<RelOptRule> postOptRules = new LinkedHashSet<>();
        // It will definitely work if it were put here
        postOptRules.add(SumConstantConvertRule.INSTANCE);
        postOptRules.add(UnionTypeCastRule.INSTANCE);
        if (kylinConfig.isConvertSumExpressionEnabled()) {
            postOptRules.addAll(HepUtils.SumExprRules);
        }
        if (kylinConfig.isConvertCountDistinctExpressionEnabled()) {
            postOptRules.addAll(HepUtils.CountDistinctExprRules);
        }

        if (kylinConfig.isAggregatePushdownEnabled()) {
            postOptRules.addAll(HepUtils.AggPushDownRules);
        }

        if (kylinConfig.isScalarSubqueryJoinEnabled()) {
            postOptRules.addAll(HepUtils.ScalarSubqueryJoinRules);
        }

        if (kylinConfig.isOptimizedSumCastDoubleRuleEnabled()) {
            postOptRules.addAll(HepUtils.SumCastDoubleRules);
        }

        if (kylinConfig.isQueryFilterReductionEnabled()) {
            postOptRules.addAll(HepUtils.FilterReductionRules);
        }

        postOptRules.add(OlapFilterJoinRule.FILTER_ON_JOIN);
        // this rule should after sum-expression and count-distinct-expression
        postOptRules.add(OlapProjectJoinTransposeRule.INSTANCE);

        RelNode transformed = HepUtils.runRuleCollection(node, postOptRules, false);
        if (transformed != node && allowAlternativeQueryPlan) {
            return Lists.newArrayList(transformed, node);
        } else {
            return Lists.newArrayList(transformed);
        }
    }

    @VisibleForTesting
    public RelRoot sqlToRelRoot(String sql) throws SqlParseException {
        return sqlConverter.convertSqlToRelNode(sql);
    }

    @VisibleForTesting
    public RelRoot optimize(RelRoot relRoot) {
        return queryOptimizer.optimize(relRoot);
    }

    /**
     * get metadata of columns of the query result
     * @param sql
     * @return
     * @throws SQLException
     */
    public List<StructField> getColumnMetaData(String sql) throws SQLException {
        try {
            beforeQuery();

            RelRoot relRoot = sqlConverter.convertSqlToRelNode(sql);

            return RelColumnMetaDataExtractor.getColumnMetadata(relRoot.validatedRowType);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            afterQuery();
        }
    }

    @VisibleForTesting
    public <T> T wrapSqlTest(Function<QueryExec, T> testFunc) {
        try {
            beforeQuery();
            return testFunc.apply(this);
        } finally {
            afterQuery();
        }
    }

    private void beforeQuery() {
        Prepare.CatalogReader.THREAD_LOCAL.set(catalogReader);
        KylinConnectionConfig.THREAD_LOCAL.set(connectionConfig);
    }

    private void afterQuery() {
        Prepare.CatalogReader.THREAD_LOCAL.remove();
        KylinConnectionConfig.THREAD_LOCAL.remove();
        FileSegments.clearFileSegFilterLocally();
        if (SparkSession.getDefaultSession().isDefined()) {
            KylinCacheFileSystem.clearAcceptCacheTimeLocally();
            SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
            sparkContext.setLocalProperty("gluten.enabledForCurrentThread", null);
        }
    }

    public void setContextVar(String name, Object val) {
        dataContext.putContextVar(name, val);
    }

    /**
     * set prepare params
     * @param idx 0-based index
     * @param val
     */
    public void setPrepareParam(int idx, Object val) {
        dataContext.setPrepareParam(idx, val);
    }

    /**
     * get default schema used in this query
     * @return
     */
    public String getDefaultSchemaName() {
        return schemaFactory.getDefaultSchema();
    }

    public CalciteSchema getRootSchema() {
        return rootSchema;
    }

    /**
     * execute query plan physically
     * @param rels list of alternative relNodes to execute.
     *             relNodes will be executed one by one and return on the first successful execution
     * @return
     */
    private ExecuteResult executeQueryPlan(List<RelNode> rels) {
        boolean routeToCalcite = QueryHelper.isConstantQueryAndCalciteEngineCapable(rels.get(0));
        dataContext.setContentQuery(routeToCalcite);
        if (!QueryContext.current().getQueryTagInfo().isAsyncQuery()
                && KapConfig.wrap(kylinConfig).runConstantQueryLocally() && routeToCalcite) {
            QueryContext.current().getQueryTagInfo().setConstantQuery(true);
            // if sparder is not enabled, or the sql can run locally, use the calcite engine
            return new CalcitePlanExec().executeToIterable(rels.get(0), dataContext);
        } else {
            return sparderQuery(rels);
        }
    }

    private ExecuteResult sparderQuery(List<RelNode> rels) {
        RuntimeException lastException = null;
        for (RelNode rel : rels) {
            try {
                ContextUtil.clearThreadLocalContexts();
                ContextUtil.clearParameter();
                return new SparderPlanExec(kylinConfig).executeToIterable(rel, dataContext);
            } catch (NoRealizationFoundException e) {
                ExecuteResult result = tryEnhancedAggPushDown(rel);
                if (result != null) {
                    return result;
                }
                lastException = e;
            } catch (IllegalArgumentException e) {
                if (e.getMessage().contains("Unsupported function name BITMAP_UUID")) {
                    if (rels.size() > 1) {
                        logger.error("Optimized relNode query fail, try origin relNode.", e);
                    }
                    lastException = e;
                } else {
                    throw e;
                }
            }
        }
        assert lastException != null;
        throw lastException;
    }

    private ExecuteResult tryEnhancedAggPushDown(RelNode rel) {
        if (NProjectManager.getProjectConfig(project).isEnhancedAggPushDownEnabled()) {
            Collection<RelOptRule> postOptRules = new LinkedHashSet<>();
            postOptRules.addAll(HepUtils.AggPushDownRules);
            return sparderQueryOptimized(rel, 1, postOptRules);
        }
        return null;
    }

    private ExecuteResult sparderQueryOptimized(RelNode relNode, int tryTimes, Collection<RelOptRule> postOptRules) {
        if (QueryContext.current().getUnmatchedJoinDigest().isEmpty() || tryTimes > MAX_TRY_TIMES_OPTIMIZED) {
            return null;
        }
        if (!QueryContext.current().isEnhancedAggPushDown() && tryTimes > 1) {
            return null;
        }
        ContextUtil.clearThreadLocalContexts();
        ContextUtil.clearParameter();
        RelAggPushDownUtil.clearCtxRelNode(relNode);
        QueryContext.current().setEnhancedAggPushDown(false);
        RelNode transformed = HepUtils.runRuleCollection(relNode, postOptRules, false);
        ContextUtil.dumpCalcitePlan("EXECUTION PLAN AFTER TABLEINDEX JOIN SNAPSHOT AGG PUSH DOWN tryTimes: " + tryTimes,
                transformed, logger);
        try {
            RelAggPushDownUtil.clearUnmatchedJoinDigest();
            return new SparderPlanExec(kylinConfig).executeToIterable(transformed, dataContext);
        } catch (NoRealizationFoundException e) {
            QueryInterruptChecker.checkThreadInterrupted(
                    "Interrupted SparderQueryOptimized NoRealizationFoundException",
                    "Current step: TableIndex join snapshot aggPushDown");
            tryTimes = tryTimes + 1;
            return sparderQueryOptimized(transformed, tryTimes, postOptRules);
        } catch (Exception e) {
            QueryInterruptChecker.checkThreadInterrupted("Interrupted SparderQueryOptimized error",
                    "Current step: Table Index join snapshot aggPushDown");
            setSparderQueryOptimizedExceptionMsg(e.getMessage());
            return null;
        }
    }

    private SimpleDataContext createDataContext(CalciteSchema rootSchema) {
        return new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(), kylinConfig);
    }

    private boolean routeToCalciteEngine(RelNode rel) {
        return isConstantQuery(rel) && isCalciteEngineCapable(rel);
    }

    /**
     * search rel node tree to see if there is any table scan node
     *
     * @param rel
     * @return
     */
    private boolean isConstantQuery(RelNode rel) {
        if (TableScan.class.isAssignableFrom(rel.getClass())) {
            return false;
        }
        for (RelNode input : rel.getInputs()) {
            if (!isConstantQuery(input)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Calcite is not capable for some constant queries, need to route to Sparder
     */
    private boolean isCalciteEngineCapable(RelNode rel) {
        if (rel instanceof Project) {
            Project projectRelNode = (Project) rel;
            if (projectRelNode.getProjects().stream().filter(RexCall.class::isInstance)
                    .anyMatch(pRelNode -> pRelNode.accept(new CalcitePlanRouterVisitor()))) {
                return false;
            }
            if (projectRelNode.getProjects() != null
                    && projectRelNode.getProjects().stream().anyMatch(this::isPlusString)) {
                return false;
            }
        }

        if (rel instanceof OlapAggregateRel) {
            OlapAggregateRel aggregateRel = (OlapAggregateRel) rel;
            if (aggregateRel.getAggCallList().stream().anyMatch(
                    aggCall -> FunctionDesc.FUNC_BITMAP_BUILD.equalsIgnoreCase(aggCall.getAggregation().getName()))) {
                return false;
            }
            if (aggregateRel.getInput() instanceof Values
                    && aggregateRel.getAggCallList().stream().anyMatch(AggregateCall::isDistinct)) {
                return false;
            }
        }

        return rel.getInputs().stream().allMatch(this::isCalciteEngineCapable);
    }

    /**
     * calcite not support 'number' + number/'number'
     *
     * @param node
     * @return
     */
    private boolean isPlusString(RexNode node) {
        if (node instanceof RexCall) {
            RexCall rexCall = (RexCall) node;
            if ("plus".equals(rexCall.getOperator().getKind().lowerName)) {
                for (RexNode operand : rexCall.operands) {
                    if (operand.getType().getFamily() == SqlTypeFamily.STRING
                            || operand.getType().getFamily() == SqlTypeFamily.CHARACTER) {
                        return true;
                    }
                }
            }
            return rexCall.getOperands().stream().anyMatch(this::isPlusString);
        }
        return false;
    }

    private SQLException newSqlException(String sql, String msg, Throwable e) {
        String diagnosticStr = "";
        String plan = " not exists";
        boolean dryRun = QueryContext.current().isDryRun();
        if (e instanceof DryRunSucceedException) {
            DryRunSucceedException d = (DryRunSucceedException) e;
            plan = d.plan();
        }
        if (dryRun) {
            StringBuilder diagnosticInfo = new StringBuilder(System.getProperty("line.separator"));
            diagnosticInfo.append("<-------------------- Dry Run Info -------------------->");

            diagnosticInfo.append(SEP).append("1. Last Exception :").append(SEP).append("  ");
            diagnosticInfo.append(msg).append(SEP);

            diagnosticInfo.append(SEP).append("2. RelNode(with ctx id) :").append(SEP).append("  ");
            diagnosticInfo.append(QueryContext.current().getLastUsedRelNode());

            diagnosticInfo.append(SEP).append("3. OLAPContext(s) and matched model(s) :");
            if (ContextUtil.getThreadLocalContexts() != null) {
                String olapMatchInfo = ContextUtil.getNativeRealizations().stream()
                        .map(r -> String.format(Locale.ROOT, " \tMatched=%s, \tIndexType=%s, \tLayoutId=%d",
                                r.getModelAlias(), r.getType(), r.getLayoutId()))
                        .collect(Collectors.joining(SEP));
                if (olapMatchInfo.length() >= 10) {
                    diagnosticInfo.append(SEP).append(olapMatchInfo).append(SEP);
                }

                for (OlapContext ctx : ContextUtil.getThreadLocalContexts()) {
                    diagnosticInfo.append(SEP);
                    diagnosticInfo.append(" Ctx=").append(ctx.getId());
                    if (ctx.getRealization() == null) {
                        diagnosticInfo.append(" is not matched by any model/snapshot, expected ")
                                .append(ctx.tipsForUser());
                    } else {
                        diagnosticInfo.append(" is matched by ").append(ctx.getRealization().getCanonicalName())
                                .append(", expected ").append(ctx.tipsForUser());
                    }
                    //                    diagnosticInfo.append(", verbose text:").append(ctx);
                }
                diagnosticInfo.append(SEP);
            }

            diagnosticInfo.append(SEP).append("4. SQL Text :").append(SEP);
            diagnosticInfo.append(sql).append(SEP);

            diagnosticInfo.append(SEP).append("5. Physical plan :").append(SEP);
            diagnosticInfo.append(plan).append(SEP);

            diagnosticInfo.append(SEP).append("<-------------------- Dry Run Info -------------------->");
            diagnosticInfo.append(SEP).append(DRY_RUN_TIP);
            diagnosticStr = diagnosticInfo.toString();
            return new SQLException(diagnosticStr, e);
        } else {
            return new SQLException("Error while executing SQL \"" + sql + "\": " + msg, e);
        }
    }

    private void processAcceptCacheTime(String hintStr) {
        if (kylinConfig.isKylinLocalCacheEnabled() && kylinConfig.isKylinFileStatusCacheEnabled()
                && SparkSession.getDefaultSession().isDefined()) {
            KylinCacheFileSystem.processAcceptCacheTimeInHintStr(hintStr);
        }
    }

    private void clearAcceptCacheTimeLocally() {
        if (kylinConfig.isKylinLocalCacheEnabled() && kylinConfig.isKylinFileStatusCacheEnabled()
                && SparkSession.getDefaultSession().isDefined()) {
            KylinCacheFileSystem.clearAcceptCacheTimeLocally();
        }
    }

    public static final String SEP = System.lineSeparator();
    public static final String DRY_RUN_TIP = SEP
            + "Tip : Dryrun is a experimental feature for user to create proper model." + SEP
            + "To enable/disable dry run, please set 'kylin.query.dryrun-enabled=true/false'" + SEP
            + "in Setting -> Advanced Settings -> Custom Project Configuration." + SEP;
}
