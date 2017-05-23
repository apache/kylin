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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;

/**
 * Created by luwei on 17-4-24.
 */
@Component("tableServiceV2")
public class TableServiceV2 extends TableService {

    private static final Logger logger = LoggerFactory.getLogger(TableServiceV2.class);

    @Autowired
    @Qualifier("modelMgmtServiceV2")
    private ModelServiceV2 modelServiceV2;

    @Autowired
    @Qualifier("projectServiceV2")
    private ProjectServiceV2 projectServiceV2;

    @Autowired
    @Qualifier("streamingMgmtService")
    private StreamingService streamingService;

    @Autowired
    @Qualifier("kafkaMgmtService")
    private KafkaConfigService kafkaConfigService;

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_MODELER + " or " + Constant.ACCESS_HAS_ROLE_ADMIN)
    public void calculateCardinalityIfNotPresent(String[] tables, String submitter) throws Exception {
        MetadataManager metaMgr = getMetadataManager();
        ExecutableManager exeMgt = ExecutableManager.getInstance(getConfig());
        for (String table : tables) {
            TableExtDesc tableExtDesc = metaMgr.getTableExt(table);
            String jobID = tableExtDesc.getJodID();
            if (null == jobID || ExecutableState.RUNNING != exeMgt.getOutput(jobID).getState()) {
                calculateCardinality(table, submitter);
            }
        }
    }

    /**
     * Generate cardinality for table This will trigger a hadoop job
     * The result will be merged into table exd info
     *
     * @param tableName
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_MODELER + " or " + Constant.ACCESS_HAS_ROLE_ADMIN)
    public void calculateCardinality(String tableName, String submitter) throws Exception {
        Message msg = MsgPicker.getMsg();

        tableName = normalizeHiveTableName(tableName);
        TableDesc table = getMetadataManager().getTableDesc(tableName);
        final TableExtDesc tableExt = getMetadataManager().getTableExt(tableName);
        if (table == null) {
            BadRequestException e = new BadRequestException(String.format(msg.getTABLE_DESC_NOT_FOUND(), tableName));
            logger.error("Cannot find table descriptor " + tableName, e);
            throw e;
        }

        DefaultChainedExecutable job = new DefaultChainedExecutable();
        //make sure the job could be scheduled when the DistributedScheduler is enable.
        job.setParam("segmentId", tableName);
        job.setName("Hive Column Cardinality calculation for table '" + tableName + "'");
        job.setSubmitter(submitter);

        String outPath = getConfig().getHdfsWorkingDirectory() + "cardinality/" + job.getId() + "/" + tableName;
        String param = "-table " + tableName + " -output " + outPath;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setMapReduceJobClass(org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityJob.class);
        step1.setMapReduceParams(param);
        step1.setParam("segmentId", tableName);

        job.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setJobClass(org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityUpdateJob.class);
        step2.setJobParams(param);
        step2.setParam("segmentId", tableName);
        job.addTask(step2);
        tableExt.setJodID(job.getId());
        getMetadataManager().saveTableExt(tableExt);

        getExecutableManager().addJob(job);
    }

    /**
     * table may referenced by several projects, and kylin only keep one copy of meta for each table,
     * that's why we have two if statement here.
     * @param tableName
     * @param project
     * @return
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public boolean unLoadHiveTableV2(String tableName, String project) throws IOException {
        Message msg = MsgPicker.getMsg();

        boolean rtn = false;
        int tableType = 0;

        //remove streaming info
        tableName = normalizeHiveTableName(tableName);
        TableDesc desc = getMetadataManager().getTableDesc(tableName);
        if (desc == null)
            return false;
        tableType = desc.getSourceType();

        if (!modelServiceV2.isTableInModel(tableName, project)) {
            removeTableFromProject(tableName, project);
            rtn = true;
        } else {
            List<String> models = modelServiceV2.getModelsUsingTable(tableName, project);
            throw new BadRequestException(String.format(msg.getTABLE_IN_USE_BY_MODEL(), models));
        }

        if (!projectServiceV2.isTableInAnyProject(tableName) && !modelServiceV2.isTableInAnyModel(tableName)) {
            unLoadHiveTable(tableName);
            rtn = true;
        }

        if (tableType == 1 && !projectServiceV2.isTableInAnyProject(tableName) && !modelServiceV2.isTableInAnyModel(tableName)) {
            StreamingConfig config = null;
            KafkaConfig kafkaConfig = null;
            try {
                config = streamingService.getStreamingManager().getStreamingConfig(tableName);
                kafkaConfig = kafkaConfigService.getKafkaConfig(tableName);
                streamingService.dropStreamingConfig(config);
                kafkaConfigService.dropKafkaConfig(kafkaConfig);
                rtn = true;
            } catch (Exception e) {
                rtn = false;
                logger.error(e.getLocalizedMessage(), e);
            }
        }
        return rtn;
    }

    public Map<String, String[]> loadHiveTables(String[] tableNames, String project, boolean isNeedProfile) throws Exception {
        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        Map<String, String[]> result = new HashMap<String, String[]>();

        String[] loaded = loadHiveTablesToProject(tableNames, project);
        result.put("result.loaded", loaded);
        Set<String> allTables = new HashSet<String>();
        for (String tableName : tableNames) {
            allTables.add(normalizeHiveTableName(tableName));
        }
        for (String loadedTableName : loaded) {
            allTables.remove(loadedTableName);
        }
        String[] unloaded = new String[allTables.size()];
        allTables.toArray(unloaded);
        result.put("result.unloaded", unloaded);
        if (isNeedProfile) {
            calculateCardinalityIfNotPresent(loaded, submitter);
        }
        return result;
    }

    public Map<String, String[]> unloadHiveTables(String[] tableNames, String project) throws IOException {
        Set<String> unLoadSuccess = Sets.newHashSet();
        Set<String> unLoadFail = Sets.newHashSet();
        Map<String, String[]> result = new HashMap<String, String[]>();

        for (String tableName : tableNames) {
            if (unLoadHiveTableV2(tableName, project)) {
                unLoadSuccess.add(tableName);
            } else {
                unLoadFail.add(tableName);
            }
        }

        result.put("result.unload.success", (String[]) unLoadSuccess.toArray(new String[unLoadSuccess.size()]));
        result.put("result.unload.fail", (String[]) unLoadFail.toArray(new String[unLoadFail.size()]));
        return result;
    }
}
