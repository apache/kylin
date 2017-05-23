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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.TableDescResponse;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("streamingMgmtService")
    private StreamingService streamingService;

    @Autowired
    @Qualifier("kafkaMgmtService")
    private KafkaConfigService kafkaConfigService;

    public List<TableDesc> getTableDescByProject(String project, boolean withExt) throws IOException {
        List<TableDesc> tables = getProjectManager().listDefinedTables(project);
        if (null == tables) {
            return Collections.emptyList();
        }
        if (withExt) {
            tables = cloneTableDesc(tables);
        }
        return tables;
    }

    public TableDesc getTableDescByName(String tableName, boolean withExt) {
        TableDesc table = getMetadataManager().getTableDesc(tableName);
        if (withExt) {
            table = cloneTableDesc(table);
        }
        return table;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String[] loadHiveTablesToProject(String[] tables, String project) throws Exception {
        // de-dup
        SetMultimap<String, String> db2tables = LinkedHashMultimap.create();
        for (String fullTableName : tables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName);
            db2tables.put(parts[0].toUpperCase(), parts[1].toUpperCase());
        }

        // load all tables first
        List<Pair<TableDesc, TableExtDesc>> allMeta = Lists.newArrayList();
        ISourceMetadataExplorer explr = SourceFactory.getDefaultSource().getSourceMetadataExplorer();
        for (Map.Entry<String, String> entry : db2tables.entries()) {
            Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue());
            TableDesc tableDesc = pair.getFirst();
            Preconditions.checkState(tableDesc.getDatabase().equals(entry.getKey()));
            Preconditions.checkState(tableDesc.getName().equals(entry.getValue()));
            Preconditions.checkState(tableDesc.getIdentity().equals(entry.getKey() + "." + entry.getValue()));
            TableExtDesc extDesc = pair.getSecond();
            Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getName()));
            allMeta.add(pair);
        }

        // do schema check
        MetadataManager metaMgr = MetadataManager.getInstance(getConfig());
        CubeManager cubeMgr = CubeManager.getInstance(getConfig());
        TableSchemaUpdateChecker checker = new TableSchemaUpdateChecker(metaMgr, cubeMgr);
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableSchemaUpdateChecker.CheckResult result = checker.allowReload(tableDesc);
            result.raiseExceptionWhenInvalid();
        }

        // save table meta
        List<String> saved = Lists.newArrayList();
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();
            
            TableDesc origTable = metaMgr.getTableDesc(tableDesc.getIdentity());
            if (origTable == null) {
                tableDesc.setUuid(UUID.randomUUID().toString());
                tableDesc.setLastModified(0);
            } else {
                tableDesc.setUuid(origTable.getUuid());
                tableDesc.setLastModified(origTable.getLastModified());
            }
            
            TableExtDesc origExt = metaMgr.getTableExt(tableDesc.getIdentity());
            if (origExt == null) {
                extDesc.setUuid(UUID.randomUUID().toString());
                extDesc.setLastModified(0);
            } else {
                extDesc.setUuid(origExt.getUuid());
                extDesc.setLastModified(origExt.getLastModified());
            }

            metaMgr.saveTableExt(extDesc);
            metaMgr.saveSourceTable(tableDesc);
            saved.add(tableDesc.getIdentity());
        }

        String[] result = (String[]) saved.toArray(new String[saved.size()]);
        syncTableToProject(result, project);
        return result;
    }

    protected void unLoadHiveTable(String tableName) throws IOException {
        tableName = normalizeHiveTableName(tableName);
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        metaMgr.removeSourceTable(tableName);
        metaMgr.removeTableExt(tableName);
    }

    private void syncTableToProject(String[] tables, String project) throws IOException {
        getProjectManager().addTableDescToProject(tables, project);
    }

    protected void removeTableFromProject(String tableName, String projectName) throws IOException {
        tableName = normalizeHiveTableName(tableName);
        getProjectManager().removeTableDescFromProject(tableName, projectName);
    }

    /**
     * table may referenced by several projects, and kylin only keep one copy of meta for each table,
     * that's why we have two if statement here.
     * @param tableName
     * @param project
     * @return
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public boolean unLoadHiveTable(String tableName, String project) {
        boolean rtn = false;
        int tableType = 0;

        //remove streaming info
        tableName = normalizeHiveTableName(tableName);
        TableDesc desc = getMetadataManager().getTableDesc(tableName);
        if (desc == null)
            return false;
        tableType = desc.getSourceType();

        try {
            if (!modelService.isTableInModel(tableName, project)) {
                removeTableFromProject(tableName, project);
                rtn = true;
            } else {
                List<String> models = modelService.getModelsUsingTable(tableName, project);
                throw new InternalErrorException("Table is already in use by models " + models);
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (!projectService.isTableInAnyProject(tableName) && !modelService.isTableInAnyModel(tableName)) {
            try {
                unLoadHiveTable(tableName);
                rtn = true;
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
                rtn = false;
            }
        }

        if (tableType == 1 && !projectService.isTableInAnyProject(tableName) && !modelService.isTableInAnyModel(tableName)) {
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

    /**
     *
     * @param desc
     * @param project
     * @throws IOException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void addStreamingTable(TableDesc desc, String project) throws IOException {
        desc.setUuid(UUID.randomUUID().toString());
        getMetadataManager().saveSourceTable(desc);
        syncTableToProject(new String[] { desc.getIdentity() }, project);
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public List<String> getHiveDbNames() throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getDefaultSource().getSourceMetadataExplorer();
        return explr.listDatabases();
    }

    /**
     *
     * @param database
     * @return
     * @throws Exception
     */
    public List<String> getHiveTableNames(String database) throws Exception {
        ISourceMetadataExplorer explr = SourceFactory.getDefaultSource().getSourceMetadataExplorer();
        return explr.listTables(database);
    }

    private TableDescResponse cloneTableDesc(TableDesc table) {
        TableExtDesc tableExtDesc = getMetadataManager().getTableExt(table.getIdentity());

        // Clone TableDesc
        TableDescResponse rtableDesc = new TableDescResponse(table);
        Map<String, Long> cardinality = new HashMap<String, Long>();
        Map<String, String> dataSourceProp = new HashMap<>();
        String scard = tableExtDesc.getCardinality();
        if (!StringUtils.isEmpty(scard)) {
            String[] cards = StringUtils.split(scard, ",");
            ColumnDesc[] cdescs = rtableDesc.getColumns();
            for (int i = 0; i < cdescs.length; i++) {
                ColumnDesc columnDesc = cdescs[i];
                if (cards.length > i) {
                    cardinality.put(columnDesc.getName(), Long.parseLong(cards[i]));
                } else {
                    logger.error("The result cardinality is not identical with hive table metadata, cardinality : " + scard + " column array length: " + cdescs.length);
                    break;
                }
            }
            rtableDesc.setCardinality(cardinality);
        }
        dataSourceProp.putAll(tableExtDesc.getDataSourceProp());
        rtableDesc.setDescExd(dataSourceProp);
        return rtableDesc;
    }

    private List<TableDesc> cloneTableDesc(List<TableDesc> tables) throws IOException {
        List<TableDesc> descs = new ArrayList<TableDesc>();
        Iterator<TableDesc> it = tables.iterator();
        while (it.hasNext()) {
            TableDesc table = it.next();
            TableDescResponse rtableDesc = cloneTableDesc(table);
            descs.add(rtableDesc);
        }

        return descs;
    }

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
        tableName = normalizeHiveTableName(tableName);
        TableDesc table = getMetadataManager().getTableDesc(tableName);
        final TableExtDesc tableExt = getMetadataManager().getTableExt(tableName);
        if (table == null) {
            IllegalArgumentException e = new IllegalArgumentException("Cannot find table descriptor " + tableName);
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

    public String normalizeHiveTableName(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase();
    }
}
