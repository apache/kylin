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

import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.DefaultSparkBuildJobHandler;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.query.util.QueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

public class AsyncQueryJob extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryJob.class);
    private static final String GLOBAL = "/_global";
    private static final String DATAFLOW = "/dataflow";
    private static final String DATAFLOW_DETAIL = "/dataflow_details";
    private static final String INDEX_PLAN = "/index_plan";
    private static final String MODEL = "/model_desc";
    private static final String TABLE = "/table";
    private static final String TABLE_EXD = "/table_exd";
    private static final String ACL = "/acl";
    private static final String[] META_DUMP_LIST = new String[] { DATAFLOW, DATAFLOW_DETAIL, INDEX_PLAN, MODEL, TABLE,
            TABLE_EXD, ACL };

    public AsyncQueryJob() {
        super();
    }

    public AsyncQueryJob(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected void initHandler() {
        sparkJobHandler = new DefaultSparkBuildJobHandler();
    }

    @Override
    protected ExecuteResult runSparkSubmit(String hadoopConf, String kylinJobJar, String appArgs) {
        val patternedLogger = new BufferedLogger(logger);
        try {
            killOrphanApplicationIfExists(getId());
            val desc = getSparkAppDesc();
            desc.setHadoopConfDir(hadoopConf);
            desc.setKylinJobJar(kylinJobJar);
            desc.setAppArgs(appArgs);
            String cmd = (String) sparkJobHandler.generateSparkCmd(KylinConfig.getInstanceFromEnv(), desc);
            CliCommandExecutor exec = getCliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult r = exec.execute(cmd, patternedLogger, getId());
            return ExecuteResult.createSucceed(r.getCmd());
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    @VisibleForTesting
    public CliCommandExecutor getCliCommandExecutor() {
        return new CliCommandExecutor();
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> overrides = config.getAsyncQuerySparkConfigOverride();

        if (StringUtils.isNotEmpty(getParam(NBatchConstants.P_QUERY_QUEUE))) {
            // async query spark queue priority: request param > project config > system config
            overrides.put("spark.yarn.queue", getParam(NBatchConstants.P_QUERY_QUEUE));
        }

        if (!overrides.containsKey("spark.driver.memory")) {
            overrides.put("spark.driver.memory", "1024m");
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            overrides.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }
        return overrides;
    }

    @Override
    protected String getJobNamePrefix() {
        return "";
    }

    @Override
    protected String getExtJar() {
        return getConfig().getKylinExtJarsPath();
    }

    @Override
    public String getId() {
        return ASYNC_QUERY_JOB_ID_PRE + super.getId();
    }

    public ExecuteResult submit(QueryParams queryParams) throws ExecuteException, JsonProcessingException {
        this.setLogPath(getSparkDriverLogHdfsPath(getConfig()));
        KylinConfig originConfig = getConfig();
        HashMap<String, String> overrideCopy = Maps.newHashMap(((KylinConfigExt) originConfig).getExtendedOverrides());
        if (StringUtils.isNotEmpty(queryParams.getSparkQueue())) {
            overrideCopy.put("kylin.query.async-query.spark-conf.spark.yarn.queue", queryParams.getSparkQueue());
        }
        KylinConfig config = KylinConfigExt.createInstance(originConfig, overrideCopy);
        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new KylinRuntimeException("Missing kylin job jar");
        }

        ObjectMapper fieldOnlyMapper = new ObjectMapper();
        fieldOnlyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        setParam(NBatchConstants.P_QUERY_PARAMS, fieldOnlyMapper.writeValueAsString(queryParams));
        setParam(NBatchConstants.P_QUERY_CONTEXT, JsonUtil.writeValueAsString(QueryContext.current()));
        setParam(NBatchConstants.P_PROJECT_NAME, getProject());
        setParam(NBatchConstants.P_QUERY_ID, QueryContext.current().getQueryId());
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_JOB_TYPE, JobTypeEnum.ASYNC_QUERY.toString());
        setParam(NBatchConstants.P_QUERY_QUEUE, queryParams.getSparkQueue());
        setDistMetaUrl(config.getJobTmpMetaStoreUrl(getProject(), getId()));

        try {
            // dump kylin.properties to HDFS
            config.setQueryHistoryUrl(config.getQueryHistoryUrl().toString());
            attachMetadataAndKylinProps(config, true);

            // dump metadata to HDFS
            List<String> metadataDumpSet = Lists.newArrayList();
            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
            metadataDumpSet.addAll(resourceStore.listResourcesRecursively(GLOBAL));
            for (String mata : META_DUMP_LIST) {
                if (resourceStore.listResourcesRecursively("/" + getProject() + mata) != null) {
                    metadataDumpSet.addAll(resourceStore.listResourcesRecursively("/" + getProject() + mata));
                }
            }
            KylinConfig configCopy = KylinConfig.createKylinConfig(config);
            configCopy.setMetadataUrl(config.getJobTmpMetaStoreUrl(getProject(), getId()).toString());
            MetadataStore.createMetadataStore(configCopy).dump(ResourceStore.getKylinMetaStore(config),
                    metadataDumpSet);
        } catch (Exception e) {
            throw new ExecuteException("kylin properties or meta dump failed", e);
        }

        return runSparkSubmit(getHadoopConfDir(), kylinJobJar,
                "-className org.apache.kylin.query.engine.AsyncQueryApplication "
                        + createArgsFileOnHDFS(config, getId()));
    }

    private String getHadoopConfDir() {
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        if (StringUtils.isNotEmpty(kylinconfig.getAsyncQueryHadoopConfDir())) {
            return kylinconfig.getAsyncQueryHadoopConfDir();
        }
        return HadoopUtil.getHadoopConfDir();
    }
}
