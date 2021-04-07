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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.RuleValidationException;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.stream.core.source.StreamingSourceConfigManager;
import org.apache.kylin.tool.migration.CompatibilityCheckRequest;
import org.apache.kylin.tool.migration.StreamTableCompatibilityCheckRequest;
import org.apache.kylin.tool.query.QueryGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * Check the pre-defined rules. If not pass, we will throw
 * {@link RuleValidationException}.
 */
public class MigrationRuleSet {

    private static final Logger logger = LoggerFactory.getLogger(MigrationRuleSet.class);

    public static final Rule DEFAULT_HIVE_TABLE_CONSISTENCY_RULE = new HiveTableConsistencyRule();
    public static final Rule DEFAULT_CUBE_STATUS_RULE = new CubeStatusRule();
    public static final Rule DEFAULT_PROJECT_EXIST_RULE = new ProjectExistenceRule();
    public static final Rule DEFAULT_AUTO_MERGE_RULE = new AutoMergePolicyRule();
    public static final Rule DEFAULT_EXPANSION_RULE = new ExpansionRateRule();
    public static final Rule DEFAULT_EMAIL_NOTIFY_RULE = new NotificationEmailRule();
    public static final Rule DEFAULT_COMPATIBLE_RULE = new CompatibleRule();
    public static final Rule DEFAULT_SEGMENT_RULE = new SegmentRule();
    public static final Rule DEFAULT_CUBE_OVERWRITE_RULE = new CubeOverwriteRule();
    public static final Rule DEFAULT_QUERY_LATENCY_RULE = new QueryLatencyRule();
    public static final Rule DEFAULT_STREAM_TABLE_CHECK_RULE = new StreamTableCompatibilityRule();

    private static List<Rule> MUSTTOPASS_RULES = Lists.newLinkedList();

    private static List<Rule> NICETOPASS_RULES = Lists.newLinkedList();

    /**
     * Register mandatory rules.
     * @param rules
     */
    public static synchronized void register(Rule... rules) {
        register(true, rules);
    }

    public static synchronized void register(boolean mandatory, Rule... rules) {
        if (mandatory) {
            for (Rule rule : rules) {
                MUSTTOPASS_RULES.add(rule);
            }
        } else {
            for (Rule rule : rules) {
                NICETOPASS_RULES.add(rule);
            }
        }
    }

    // initialize default rules
    static {
        register(DEFAULT_HIVE_TABLE_CONSISTENCY_RULE, DEFAULT_CUBE_STATUS_RULE, DEFAULT_PROJECT_EXIST_RULE,
                 DEFAULT_EMAIL_NOTIFY_RULE, DEFAULT_SEGMENT_RULE, DEFAULT_CUBE_OVERWRITE_RULE,
                 DEFAULT_COMPATIBLE_RULE, DEFAULT_STREAM_TABLE_CHECK_RULE);

        register(false, DEFAULT_AUTO_MERGE_RULE, DEFAULT_EXPANSION_RULE, DEFAULT_QUERY_LATENCY_RULE);
    }

    /**
     * @param ctx
     * @return warn message if fail to pass some nice to have rules
     * @throws RuleValidationException
     */
    public static String apply(Context ctx) throws RuleValidationException {
        for (Rule rule : MUSTTOPASS_RULES) {
            rule.apply(ctx);
        }
        StringBuilder sb = new StringBuilder();
        for (Rule rule : NICETOPASS_RULES) {
            try {
                rule.apply(ctx);
            } catch (RuleValidationException e) {
                sb.append(e.getMessage());
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    public interface Rule {
        /**
         * Apply the rule, success if no exception is thrown.
         *
         * @param ctx
         * @throws RuleValidationException
         *             if broke this rule
         */
        public void apply(Context ctx) throws RuleValidationException;
    }

    private static class ProjectExistenceRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            // code from CubeCopyCLI.java
            ResourceStore dstStore = ctx.getTargetResourceStore();
            String projectResPath = ProjectInstance.concatResourcePath(ctx.getTgtProjectName());
            try {
                if (!dstStore.exists(projectResPath)) {
                    throw new RuleValidationException("The target project " + ctx.getTgtProjectName()
                            + " does not exist on " + ctx.getTargetAddress());
                }
            } catch (RuleValidationException e) {
                throw e;
            } catch (IOException e) {
                throw new RuleValidationException("Internal error: " + e.getMessage(), e);
            }
        }
    }

    private static class AutoMergePolicyRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            CubeDesc cubeDesc = ctx.getCubeInstance().getDescriptor();
            long[] timeRanges = cubeDesc.getAutoMergeTimeRanges();
            if (timeRanges == null || timeRanges.length == 0) {
                throw new RuleValidationException(String.format(Locale.ROOT,
                        "Auto merge time range for cube %s is not set.", cubeDesc.getName()));
            }
        }
    }

    private static class ExpansionRateRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            int expansionRateThr = KylinConfig.getInstanceFromEnv().getMigrationRuleExpansionRateThreshold();

            CubeInstance cube = ctx.getCubeInstance();
            if (cube.getInputRecordSizeBytes() == 0 || cube.getSizeKB() == 0) {
                logger.warn("cube {} has zero input record size.", cube.getName());
                throw new RuleValidationException(String.format(Locale.ROOT, "Cube %s is not built.", cube.getName()));
            }
            double expansionRate = cube.getSizeKB() * 1024.0 / cube.getInputRecordSizeBytes();
            if (expansionRate > expansionRateThr) {
                logger.info(
                        "cube {}, size_kb {}, cube record size {}, cube expansion rate {} larger than threshold {}.",
                        cube.getName(), cube.getSizeKB(), cube.getInputRecordSizeBytes(), expansionRate,
                        expansionRateThr);
                throw new RuleValidationException(
                        "ExpansionRateRule: failed on expansion rate check with exceeding " + expansionRateThr);
            }
        }
    }

    private static class NotificationEmailRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            CubeDesc cubeDesc = ctx.getCubeInstance().getDescriptor();
            List<String> notifyList = cubeDesc.getNotifyList();
            if (notifyList == null || notifyList.size() == 0) {
                throw new RuleValidationException("Cube email notification list is not set or empty.");
            }
        }
    }

    private static class CompatibleRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            try {
                checkSchema(ctx);
            } catch (Exception e) {
                throw new RuleValidationException(e.getMessage(), e);
            }
        }

        public void checkSchema(Context ctx) throws IOException {
            Set<TableDesc> tableSet = Sets.newHashSet();
            for (TableRef tableRef : ctx.getCubeInstance().getModel().getAllTables()) {
                tableSet.add(tableRef.getTableDesc());
            }

            List<String> tableDataList = Lists.newArrayList();
            for (TableDesc table : tableSet) {
                tableDataList.add(JsonUtil.writeValueAsIndentString(table));
            }

            DataModelDesc model = ctx.getCubeInstance().getModel();
            String modelDescData = JsonUtil.writeValueAsIndentString(model);

            CompatibilityCheckRequest request = new CompatibilityCheckRequest();
            request.setProjectName(ctx.getTgtProjectName());
            request.setTableDescDataList(tableDataList);
            request.setModelDescData(modelDescData);

            String jsonRequest = JsonUtil.writeValueAsIndentString(request);
            RestClient client = new RestClient(ctx.getTargetAddress());
            client.checkCompatibility(jsonRequest);
        }
    }

    private static class SegmentRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            List<CubeSegment> segments = ctx.getCubeInstance().getSegments(SegmentStatusEnum.READY);
            if (segments == null || segments.size() == 0) {
                throw new RuleValidationException("No built segment found.");
            }
        }
    }

    private static class CubeOverwriteRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            ResourceStore dstStore = ctx.getTargetResourceStore();
            CubeInstance cube = ctx.getCubeInstance();
            try {
                if (dstStore.exists(cube.getResourcePath()))
                    throw new RuleValidationException("The cube named " + cube.getName()
                            + " already exists on target metadata store. Please delete it firstly and try again");
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                throw new RuleValidationException(e.getMessage(), e);
            }
        }
    }

    private static class CubeStatusRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            CubeInstance cube = ctx.getCubeInstance();
            RealizationStatusEnum status = cube.getStatus();
            if (status != RealizationStatusEnum.READY) {
                throw new RuleValidationException("The cube named " + cube.getName() + " is not in READY state.");
            }
        }
    }

    private static class QueryLatencyRule implements Rule {

        @Override
        public void apply(MigrationRuleSet.Context ctx) throws RuleValidationException {
            logger.info("QueryLatencyRule started.");
            CubeInstance cube = ctx.getCubeInstance();

            int latency = KylinConfig.getInstanceFromEnv().getMigrationRuleQueryLatency();
            int iteration = KylinConfig.getInstanceFromEnv().getMigrationRuleQueryEvaluationIteration();
            int maxDimension = cube.getConfig().getMigrationRuleQueryGeneratorMaxDimensions();

            try {
                List<String> queries = QueryGenerator.generateQueryList(cube.getDescriptor(), iteration, maxDimension);
                assert queries.size() > 0;
                long avg = executeQueries(queries, ctx);
                logger.info("QueryLatencyRule ended: average time cost " + avg + "ms.");
                if (avg > latency) {
                    throw new RuleValidationException(
                            "Failed on query latency check with average cost " + avg + " exceeding " + latency + "ms.");
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                if (e instanceof RuleValidationException) {
                    throw (RuleValidationException) e;
                } else {
                    throw new RuleValidationException(e.getMessage(), e);
                }
            }
        }

        private long executeQueries(final List<String> queries, final Context ctx) throws Exception {
            int maxThreads = KylinConfig.getInstanceFromEnv().getMigrationRuleQueryLatencyMaxThreads();
            int threadNum = Math.min(maxThreads, queries.size());
            ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
            CompletionService<Long> completionService = new ExecutorCompletionService<Long>(threadPool);
            final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
            long start = System.currentTimeMillis();
            for (final String query : queries) {
                completionService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        SecurityContextHolder.getContext().setAuthentication(auth);
                        SQLRequest sqlRequest = new SQLRequest();
                        sqlRequest.setProject(ctx.getSrcProjectName());
                        sqlRequest.setSql(query);
                        SQLResponse sqlResponse = ctx.getQueryService().doQueryWithCache(sqlRequest, false);
                        if (sqlResponse.getIsException()) {
                            throw new RuleValidationException(sqlResponse.getExceptionMessage());
                        }
                        return sqlResponse.getDuration();
                    }

                });
            }
            long timeCostSum = 0L;
            for (int i = 0; i < queries.size(); ++i) {
                try {
                    timeCostSum += completionService.take().get();
                } catch (InterruptedException | ExecutionException e) {
                    threadPool.shutdownNow();
                    throw e;
                }
            }
            long end = System.currentTimeMillis();
            logger.info("Execute" + queries.size() + " queries took " + (end - start) + " ms, query time cost sum "
                    + timeCostSum + " ms.");
            return timeCostSum / queries.size();
        }
    }

    // check if table schema on Kylin is updated to date with external Hive table
    private static class HiveTableConsistencyRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            // check table type
            CubeInstance cube = ctx.getCubeInstance();
            boolean isStreamTable = cube.getDescriptor().getModel().getRootFactTable().getTableDesc().isStreamingTable();
            if (isStreamTable) {
                // check lambda
                if (!cube.getDescriptor().getModel().getRootFactTable().getTableDesc().isLambdaTable()) {
                    // streaming table without lambda doesn't need to check hive table
                    return;
                }
            }

            // de-dup
            SetMultimap<String, String> db2tables = LinkedHashMultimap.create();
            for (TableRef tableRef : ctx.getCubeInstance().getModel().getAllTables()) {
                db2tables.put(tableRef.getTableDesc().getDatabase().toUpperCase(Locale.ROOT),
                        tableRef.getTableDesc().getName().toUpperCase(Locale.ROOT));
            }

            // load all tables first
            List<Pair<TableDesc, TableExtDesc>> allMeta = Lists.newArrayList();
            ISourceMetadataExplorer explr = SourceManager.getDefaultSource().getSourceMetadataExplorer();
            try {
                for (Map.Entry<String, String> entry : db2tables.entries()) {
                    Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue(),
                            ctx.getSrcProjectName());
                    TableDesc tableDesc = pair.getFirst();
                    Preconditions.checkState(tableDesc.getDatabase().equals(entry.getKey()));
                    Preconditions.checkState(tableDesc.getName().equals(entry.getValue()));
                    Preconditions.checkState(tableDesc.getIdentity().equals(entry.getKey() + "." + entry.getValue()));
                    TableExtDesc extDesc = pair.getSecond();
                    Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getIdentity()));
                    allMeta.add(pair);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                throw new RuleValidationException(
                        "Internal error when checking HiveTableConsistencyRule: " + e.getMessage());
            }

            // do schema check
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            TableSchemaUpdateChecker checker = new TableSchemaUpdateChecker(TableMetadataManager.getInstance(config),
                CubeManager.getInstance(config), DataModelManager.getInstance(config),
                StreamingSourceConfigManager.getInstance(config));
            for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
                try {
                    TableSchemaUpdateChecker.CheckResult result = checker.allowReload(pair.getFirst(),
                            ctx.getSrcProjectName());
                    result.raiseExceptionWhenInvalid();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    throw new RuleValidationException("Table " + pair.getFirst().getIdentity()
                            + " has incompatible changes on Hive, please reload the hive table and update your model/cube if needed.");
                }
            }
            logger.info("Cube " + ctx.getCubeInstance().getName() + " Hive table consistency check passed.");
        }

    }

    private static class StreamTableCompatibilityRule implements Rule {

        @Override
        public void apply(Context ctx) throws RuleValidationException {
            try {
                checkStreamTableSchema(ctx);
            } catch (IOException e) {
                throw new RuleValidationException(e.getMessage(), e);
            }
        }

        public void checkStreamTableSchema(Context ctx) throws IOException {
            // check stream kylin table
            TableDesc tableDesc = ctx.getCubeInstance().getModel().getRootFactTable().getTableDesc();
            if (!tableDesc.isStreamingTable()) {
                return;
            }
            logger.info("check the stream table schema, cubename {}, project {}, lambda {}",
                    ctx.cubeInstance.getName(), ctx.cubeInstance.getProject(), tableDesc.isLambdaTable());

            // get stream source config
            StreamingSourceConfig streamingSourceConfig = StreamingSourceConfigManager
                    .getInstance(ctx.cubeInstance.getConfig())
                    .getConfig(tableDesc.getIdentity(), tableDesc.getProject());

            StreamTableCompatibilityCheckRequest streamRequest = new StreamTableCompatibilityCheckRequest();
            streamRequest.setProjectName(ctx.getTgtProjectName());
            streamRequest.setTableDesc(JsonUtil.writeValueAsIndentString(tableDesc));
            streamRequest.setStreamSource(JsonUtil.writeValueAsIndentString(streamingSourceConfig));

            String jsonRequest = JsonUtil.writeValueAsIndentString(streamRequest);
            RestClient client = new RestClient(ctx.getTargetAddress());
            client.checkStreamTableCompatibility(jsonRequest);
            }
        }

    public static class Context {
        private final QueryService queryService;
        private final CubeInstance cubeInstance;
        private final String targetAddress; // the target kylin host with port
        private final ResourceStore targetResourceStore;
        private final String tgtProjectName; // the target project name
        private final String srcProjectName; // the source project name

        public Context(QueryService queryService, CubeInstance cubeInstance, String targetHost, String tgtProjectName) {
            this.queryService = queryService;
            this.cubeInstance = cubeInstance;
            this.targetAddress = targetHost;
            KylinConfig targetConfig = KylinConfig.createInstanceFromUri(targetHost);
            this.targetResourceStore = ResourceStore.getStore(targetConfig);
            this.tgtProjectName = tgtProjectName;

            List<ProjectInstance> projList = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .findProjects(cubeInstance.getType(), cubeInstance.getName());
            if (projList.size() != 1) {
                throw new InternalErrorException("Cube " + cubeInstance.getName()
                        + " should belong to only one project. However, it's belong to " + projList);
            }
            this.srcProjectName = projList.get(0).getName();
        }

        public QueryService getQueryService() {
            return queryService;
        }

        public CubeInstance getCubeInstance() {
            return cubeInstance;
        }

        public String getTargetAddress() {
            return targetAddress;
        }

        public ResourceStore getTargetResourceStore() {
            return targetResourceStore;
        }

        public String getTgtProjectName() {
            return tgtProjectName;
        }

        public String getSrcProjectName() {
            return srcProjectName;
        }
    }
}
