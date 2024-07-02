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

import static org.apache.kylin.common.persistence.MetadataType.ACL;
import static org.apache.kylin.common.persistence.MetadataType.COMPUTE_COLUMN;
import static org.apache.kylin.common.persistence.MetadataType.DATAFLOW;
import static org.apache.kylin.common.persistence.MetadataType.FUSION_MODEL;
import static org.apache.kylin.common.persistence.MetadataType.INDEX_PLAN;
import static org.apache.kylin.common.persistence.MetadataType.LAYOUT;
import static org.apache.kylin.common.persistence.MetadataType.MODEL;
import static org.apache.kylin.common.persistence.MetadataType.NON_GLOBAL_METADATA_TYPE;
import static org.apache.kylin.common.persistence.MetadataType.OBJECT_ACL;
import static org.apache.kylin.common.persistence.MetadataType.PROJECT;
import static org.apache.kylin.common.persistence.MetadataType.RESOURCE_GROUP;
import static org.apache.kylin.common.persistence.MetadataType.SEGMENT;
import static org.apache.kylin.common.persistence.MetadataType.SQL_BLACKLIST;
import static org.apache.kylin.common.persistence.MetadataType.TABLE_EXD;
import static org.apache.kylin.common.persistence.MetadataType.TABLE_INFO;
import static org.apache.kylin.common.persistence.MetadataType.USER_GLOBAL_ACL;
import static org.apache.kylin.common.persistence.MetadataType.USER_GROUP;
import static org.apache.kylin.query.util.AsyncQueryUtil.ASYNC_QUERY_JOB_ID_PRE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.extension.KylinInfoExtension;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.spark.job.DefaultSparkBuildJobHandler;
import org.apache.kylin.engine.spark.job.NSparkExecutable;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.util.DumpInfo;
import org.apache.kylin.util.MetadataDumpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import lombok.val;

public class AsyncQueryJob extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryJob.class);
    private static final MetadataType[] META_DUMP_LIST = new MetadataType[] { DATAFLOW, LAYOUT, INDEX_PLAN, MODEL,
            TABLE_INFO, TABLE_EXD, USER_GLOBAL_ACL, ACL, OBJECT_ACL, PROJECT, COMPUTE_COLUMN, SEGMENT, USER_GROUP,
            SQL_BLACKLIST, FUSION_MODEL, RESOURCE_GROUP };

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
        return ExecutableUtil.removeGultenParams(overrides);
    }

    @Override
    protected String getJobNamePrefix() {
        return "";
    }

    @Override
    protected String getExtJar() {
        return getConfig().getKylinExtJarsPath(false);
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
            dumpKylinProps(config);
            // dump metadata to HDFS
            DumpInfo dumpInfo = generateDumpInfo(config, DumpInfo.DumpType.ASYNC_QUERY);
            MetadataDumpUtil.dumpMetadata(dumpInfo);
        } catch (Exception e) {
            throw new ExecuteException("kylin properties or meta dump failed", e);
        }

        return runSparkSubmit(getHadoopConfDir(), kylinJobJar,
                "-className org.apache.kylin.query.engine.AsyncQueryApplication "
                        + createArgsFileOnHDFS(config, getId()));
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
        Set<String> metadataDumpSet = new HashSet<>();
        for (MetadataType mata : META_DUMP_LIST) {
            NavigableSet<String> metadata;
            if (NON_GLOBAL_METADATA_TYPE.contains(mata)) {
                metadata = resourceStore.listResourcesRecursively(mata.name(),
                        RawResourceFilter.equalFilter("project", getProject()));
            } else {
                metadata = resourceStore.listResourcesRecursively(mata.name());
            }

            if (metadata != null) {
                metadataDumpSet.addAll(metadata);
            }
        }
        return metadataDumpSet;
    }

    private String getHadoopConfDir() {
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        if (StringUtils.isNotEmpty(kylinconfig.getAsyncQueryHadoopConfDir())) {
            return kylinconfig.getAsyncQueryHadoopConfDir();
        }
        return HadoopUtil.getHadoopConfDir();
    }

    @Override
    public void modifyDump(Properties props) {
        super.modifyDump(props);
        if (!KylinInfoExtension.getFactory().checkKylinInfo()) {
            props.setProperty("kylin.streaming.enabled", KylinConfig.FALSE);
        }
        props.put("kylin.internal-table-enabled", KylinConfig.FALSE);
        props.remove("kylin.storage.columnar.spark-conf.spark.sql.catalog.INTERNAL_CATALOG");
    }
}
