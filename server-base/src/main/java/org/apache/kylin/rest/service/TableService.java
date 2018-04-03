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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.TableDescResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityJob;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityUpdateJob;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("streamingMgmtService")
    private StreamingService streamingService;

    @Autowired
    @Qualifier("kafkaMgmtService")
    private KafkaConfigService kafkaConfigService;

    @Autowired
    private AclEvaluate aclEvaluate;

    public List<TableDesc> getTableDescByProject(String project, boolean withExt) throws IOException {
        aclEvaluate.checkProjectReadPermission(project);
        List<TableDesc> tables = getProjectManager().listDefinedTables(project);
        if (null == tables) {
            return Collections.emptyList();
        }
        if (withExt) {
            aclEvaluate.checkProjectWritePermission(project);
            tables = cloneTableDesc(tables, project);
        }
        return tables;
    }

    public TableDesc getTableDescByName(String tableName, boolean withExt, String prj) {
        aclEvaluate.checkProjectReadPermission(prj);
        TableDesc table = getTableManager().getTableDesc(tableName, prj);
        if (withExt) {
            aclEvaluate.checkProjectWritePermission(prj);
            table = cloneTableDesc(table, prj);
        }
        return table;
    }

    public String[] loadHiveTablesToProject(String[] tables, String project) throws Exception {
        aclEvaluate.checkProjectAdminPermission(project);
        List<Pair<TableDesc, TableExtDesc>> allMeta = getAllMeta(tables, project);
        return loadHiveTablesToProject(project, allMeta);
    }

    String[] loadHiveTablesToProject(String project, List<Pair<TableDesc, TableExtDesc>> allMeta) throws Exception {
        // do schema check
        TableMetadataManager metaMgr = getTableManager();
        CubeManager cubeMgr = getCubeManager();
        TableSchemaUpdateChecker checker = new TableSchemaUpdateChecker(metaMgr, cubeMgr);
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableSchemaUpdateChecker.CheckResult result = checker.allowReload(tableDesc, project);
            result.raiseExceptionWhenInvalid();
        }

        // save table meta
        List<String> saved = Lists.newArrayList();
        for (Pair<TableDesc, TableExtDesc> pair : allMeta) {
            TableDesc tableDesc = pair.getFirst();
            TableExtDesc extDesc = pair.getSecond();

            TableDesc origTable = metaMgr.getTableDesc(tableDesc.getIdentity(), project);
            if (origTable == null || origTable.getProject() == null) {
                tableDesc.setUuid(UUID.randomUUID().toString());
                tableDesc.setLastModified(0);
            } else {
                tableDesc.setUuid(origTable.getUuid());
                tableDesc.setLastModified(origTable.getLastModified());
            }
            metaMgr.saveSourceTable(tableDesc, project);

            TableExtDesc origExt = metaMgr.getTableExt(tableDesc.getIdentity(), project);
            if (origExt == null || origExt.getProject() == null) {
                extDesc.setUuid(UUID.randomUUID().toString());
                extDesc.setLastModified(0);
            } else {
                extDesc.setUuid(origExt.getUuid());
                extDesc.setLastModified(origExt.getLastModified());
            }
            extDesc.init(project);
            metaMgr.saveTableExt(extDesc, project);

            saved.add(tableDesc.getIdentity());
        }

        String[] result = (String[]) saved.toArray(new String[saved.size()]);
        addTableToProject(result, project);
        return result;
    }

    private List<Pair<TableDesc, TableExtDesc>> getAllMeta(String[] tables, String project) throws Exception {
        // de-dup
        SetMultimap<String, String> db2tables = LinkedHashMultimap.create();
        for (String fullTableName : tables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName);
            db2tables.put(parts[0], parts[1]);
        }

        // load all tables first
        List<Pair<TableDesc, TableExtDesc>> allMeta = Lists.newArrayList();
        ProjectInstance projectInstance = getProjectManager().getProject(project);
        ISourceMetadataExplorer explr = SourceManager.getSource(projectInstance).getSourceMetadataExplorer();
        for (Map.Entry<String, String> entry : db2tables.entries()) {
            Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(entry.getKey(), entry.getValue(), project);
            TableDesc tableDesc = pair.getFirst();
            Preconditions.checkState(tableDesc.getDatabase().equals(entry.getKey().toUpperCase()));
            Preconditions.checkState(tableDesc.getName().equals(entry.getValue().toUpperCase()));
            Preconditions.checkState(tableDesc.getIdentity()
                    .equals(entry.getKey().toUpperCase() + "." + entry.getValue().toUpperCase()));
            TableExtDesc extDesc = pair.getSecond();
            Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getIdentity()));
            allMeta.add(pair);
        }
        return allMeta;
    }

    public Map<String, String[]> loadHiveTables(String[] tableNames, String project, boolean isNeedProfile)
            throws Exception {
        aclEvaluate.checkProjectAdminPermission(project);
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
            calculateCardinalityIfNotPresent(loaded, submitter, project);
        }
        return result;
    }

    public Map<String, String[]> unloadHiveTables(String[] tableNames, String project) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        Set<String> unLoadSuccess = Sets.newHashSet();
        Set<String> unLoadFail = Sets.newHashSet();
        Map<String, String[]> result = new HashMap<String, String[]>();

        for (String tableName : tableNames) {
            if (unloadHiveTable(tableName, project)) {
                unLoadSuccess.add(tableName);
            } else {
                unLoadFail.add(tableName);
            }
        }

        result.put("result.unload.success", (String[]) unLoadSuccess.toArray(new String[unLoadSuccess.size()]));
        result.put("result.unload.fail", (String[]) unLoadFail.toArray(new String[unLoadFail.size()]));
        return result;
    }

    private void addTableToProject(String[] tables, String project) throws IOException {
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
    public boolean unloadHiveTable(String tableName, String project) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        Message msg = MsgPicker.getMsg();

        boolean rtn = false;
        int tableType = 0;

        tableName = normalizeHiveTableName(tableName);
        TableDesc desc = getTableManager().getTableDesc(tableName, project);

        // unload of legacy global table is not supported for now
        if (desc == null || desc.getProject() == null) {
            logger.warn("Unload Table {} in Project {} failed, could not find TableDesc or related Project", tableName,
                    project);
            return false;
        }

        tableType = desc.getSourceType();

        if (!modelService.isTableInModel(desc, project)) {
            removeTableFromProject(tableName, project);
            rtn = true;
        } else {
            List<String> models = modelService.getModelsUsingTable(desc, project);
            throw new BadRequestException(String.format(msg.getTABLE_IN_USE_BY_MODEL(), models));
        }

        // it is a project local table, ready to remove since no model is using it within the project
        TableMetadataManager metaMgr = getTableManager();
        metaMgr.removeTableExt(tableName, project);
        metaMgr.removeSourceTable(tableName, project);

        // remove streaming info
        if (tableType == 1) {
            StreamingConfig config = null;
            KafkaConfig kafkaConfig = null;
            try {
                config = streamingService.getStreamingManager().getStreamingConfig(tableName);
                kafkaConfig = kafkaConfigService.getKafkaConfig(tableName, project);
                streamingService.dropStreamingConfig(config, project);
                kafkaConfigService.dropKafkaConfig(kafkaConfig, project);
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
    public void addStreamingTable(TableDesc desc, String project) throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        desc.setUuid(UUID.randomUUID().toString());
        getTableManager().saveSourceTable(desc, project);
        addTableToProject(new String[] { desc.getIdentity() }, project);
    }

    /**
     *
     * @param project
     * @return
     * @throws Exception
     */
    public List<String> getSourceDbNames(String project) throws Exception {
        ISourceMetadataExplorer explr = SourceManager.getInstance(getConfig()).getProjectSource(project)
                .getSourceMetadataExplorer();
        return explr.listDatabases();
    }

    /**
     *
     * @param project
     * @param database
     * @return
     * @throws Exception
     */
    public List<String> getSourceTableNames(String project, String database) throws Exception {
        ISourceMetadataExplorer explr = SourceManager.getInstance(getConfig()).getProjectSource(project)
                .getSourceMetadataExplorer();
        return explr.listTables(database);
    }

    private TableDescResponse cloneTableDesc(TableDesc table, String prj) {
        TableExtDesc tableExtDesc = getTableManager().getTableExt(table.getIdentity(), prj);

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
                    logger.error("The result cardinality is not identical with hive table metadata, cardinality : "
                            + scard + " column array length: " + cdescs.length);
                    break;
                }
            }
            rtableDesc.setCardinality(cardinality);
        }
        dataSourceProp.putAll(tableExtDesc.getDataSourceProp());
        rtableDesc.setDescExd(dataSourceProp);
        return rtableDesc;
    }

    private List<TableDesc> cloneTableDesc(List<TableDesc> tables, String prj) throws IOException {
        List<TableDesc> descs = new ArrayList<TableDesc>();
        Iterator<TableDesc> it = tables.iterator();
        while (it.hasNext()) {
            TableDesc table = it.next();
            TableDescResponse rtableDesc = cloneTableDesc(table, prj);
            descs.add(rtableDesc);
        }

        return descs;
    }

    public void calculateCardinalityIfNotPresent(String[] tables, String submitter, String prj) throws Exception {
        TableMetadataManager metaMgr = getTableManager();
        ExecutableManager exeMgt = ExecutableManager.getInstance(getConfig());
        for (String table : tables) {
            TableExtDesc tableExtDesc = metaMgr.getTableExt(table, prj);
            String jobID = tableExtDesc.getJodID();
            if (null == jobID || ExecutableState.RUNNING != exeMgt.getOutput(jobID).getState()) {
                calculateCardinality(table, submitter, prj);
            }
        }
    }

    /**
     * Generate cardinality for table This will trigger a hadoop job
     * The result will be merged into table exd info
     *
     * @param tableName
     */
    public void calculateCardinality(String tableName, String submitter, String prj) throws Exception {
        aclEvaluate.checkProjectWritePermission(prj);
        Message msg = MsgPicker.getMsg();

        tableName = normalizeHiveTableName(tableName);
        TableDesc table = getTableManager().getTableDesc(tableName, prj);
        final TableExtDesc tableExt = getTableManager().getTableExt(tableName, prj);
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
        String param = "-table " + tableName + " -output " + outPath + " -project " + prj;

        MapReduceExecutable step1 = new MapReduceExecutable();

        step1.setMapReduceJobClass(HiveColumnCardinalityJob.class);
        step1.setMapReduceParams(param);
        step1.setParam("segmentId", tableName);

        job.addTask(step1);

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setJobClass(HiveColumnCardinalityUpdateJob.class);
        step2.setJobParams(param);
        step2.setParam("segmentId", tableName);
        job.addTask(step2);
        tableExt.setJodID(job.getId());
        getTableManager().saveTableExt(tableExt, prj);

        getExecutableManager().addJob(job);
    }

    public String normalizeHiveTableName(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase();
    }
}
