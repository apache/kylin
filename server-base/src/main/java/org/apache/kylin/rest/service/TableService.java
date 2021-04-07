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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.IExtLookupTableCache.CacheState;
import org.apache.kylin.dict.lookup.LookupProviderFactory;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.spark.SparkColumnCardinality;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.job.execution.CardinalityExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.response.TableDescResponse;
import org.apache.kylin.rest.response.TableSnapshotResponse;
import org.apache.kylin.rest.service.update.TableSchemaUpdateMapping;
import org.apache.kylin.rest.service.update.TableSchemaUpdater;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityJob;
import org.apache.kylin.source.hive.cardinality.HiveColumnCardinalityUpdateJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.base.Predicate;
import org.apache.kylin.shaded.com.google.common.collect.Iterables;
import org.apache.kylin.shaded.com.google.common.collect.LinkedHashMultimap;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.SetMultimap;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import static org.apache.kylin.job.constant.ExecutableConstants.STEP_NAME_CALCULATE_COLUMN_CARDINALITY;
import static org.apache.kylin.job.constant.ExecutableConstants.STEP_NAME_SAVE_CARDINALITY_TO_METADATA;

@Component("tableService")
public class TableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(TableService.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

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

    public TableSchemaUpdateChecker getSchemaUpdateChecker() {
        return new TableSchemaUpdateChecker(getTableManager(), getCubeManager(), getDataModelManager(), getStreamingSourceConfigManager());
    }

    public void checkStreamTableCompatibility(String project, TableDesc tableDesc) {
        TableSchemaUpdateChecker.CheckResult result = getSchemaUpdateChecker().streamTableCheckCompatibility(tableDesc, project);
        result.raiseExceptionWhenInvalid();
    }

    public void checkTableCompatibility(String prj, TableDesc tableDesc) {
        TableSchemaUpdateChecker.CheckResult result = getSchemaUpdateChecker().allowReload(tableDesc, prj);
        result.raiseExceptionWhenInvalid();
    }

    public void checkHiveTableCompatibility(String prj, TableDesc tableDesc) throws Exception {
        Preconditions.checkNotNull(tableDesc.getDatabase());
        Preconditions.checkNotNull(tableDesc.getName());

        String database = tableDesc.getDatabase().toUpperCase(Locale.ROOT);
        String tableName = tableDesc.getName().toUpperCase(Locale.ROOT);
        ProjectInstance projectInstance = getProjectManager().getProject(prj);
        ISourceMetadataExplorer explr = SourceManager.getSource(projectInstance).getSourceMetadataExplorer();

        TableDesc hiveTableDesc;
        try {
            Pair<TableDesc, TableExtDesc> pair = explr.loadTableMetadata(database, tableName, prj);
            hiveTableDesc = pair.getFirst();
        } catch (Exception e) {
            logger.error("Fail to get metadata for hive table {} due to ", tableDesc.getIdentity(), e);
            throw new RuntimeException("Fail to get metadata for hive table " + tableDesc.getIdentity());
        }

        TableSchemaUpdateChecker.CheckResult result = getSchemaUpdateChecker().allowMigrate(tableDesc, hiveTableDesc);
        result.raiseExceptionWhenInvalid();
    }
    
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

    /**
     * @return all loaded table names
     * @throws Exception on error
     */
    public String[] loadHiveTablesToProject(String[] hiveTables, String project) throws Exception {
        aclEvaluate.checkProjectAdminPermission(project);
        List<Pair<TableDesc, TableExtDesc>> allMeta = extractHiveTableMeta(hiveTables, project);
        return loadTablesToProject(allMeta, project);
    }

    /**
     * @return all loaded table names
     * @throws Exception on error
     */
    public String[] loadTableToProject(TableDesc tableDesc, TableExtDesc extDesc, String project) throws IOException {
        return loadTablesToProject(Lists.newArrayList(Pair.newPair(tableDesc, extDesc)), project);
    }

    private String[] loadTablesToProject(List<Pair<TableDesc, TableExtDesc>> allMeta, String project)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        // do schema check
        TableMetadataManager metaMgr = getTableManager();
        CubeManager cubeMgr = getCubeManager();
        TableSchemaUpdateChecker checker = new TableSchemaUpdateChecker(metaMgr,
                cubeMgr,
                getDataModelManager(),
                getStreamingSourceConfigManager());
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
                tableDesc.setUuid(RandomUtil.randomUUID().toString());
                tableDesc.setLastModified(0);
            } else {
                tableDesc.setUuid(origTable.getUuid());
                tableDesc.setLastModified(origTable.getLastModified());
            }
            metaMgr.saveSourceTable(tableDesc, project);

            if (extDesc != null) {
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
            }

            saved.add(tableDesc.getIdentity());
        }

        String[] result = (String[]) saved.toArray(new String[saved.size()]);
        addTableToProject(result, project);
        return result;
    }

    public List<Pair<TableDesc, TableExtDesc>> extractHiveTableMeta(String[] tables, String project) throws Exception { // de-dup
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
            Preconditions.checkState(tableDesc.getDatabase().equals(entry.getKey().toUpperCase(Locale.ROOT)));
            Preconditions.checkState(tableDesc.getName().equals(entry.getValue().toUpperCase(Locale.ROOT)));
            Preconditions.checkState(tableDesc.getIdentity()
                    .equals(entry.getKey().toUpperCase(Locale.ROOT) + "." + entry.getValue().toUpperCase(Locale.ROOT)));
            TableExtDesc extDesc = pair.getSecond();
            Preconditions.checkState(tableDesc.getIdentity().equals(extDesc.getIdentity()));
            allMeta.add(pair);
        }
        return allMeta;
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
     * @param  needRemoveStreamInfo
     * @return
     */
    public boolean unloadHiveTable(String tableName, String project, boolean needRemoveStreamInfo) throws IOException {
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

        if (!modelService.isTableInModel(desc, project)) {
            removeTableFromProject(tableName, project);
            rtn = true;
        } else {
            List<String> models = modelService.getModelsUsingTable(desc, project);
            throw new BadRequestException(String.format(Locale.ROOT, msg.getTABLE_IN_USE_BY_MODEL(), models));
        }

        // it is a project local table, ready to remove since no model is using it within the project
        TableMetadataManager metaMgr = getTableManager();
        metaMgr.removeTableExt(tableName, project);
        metaMgr.removeSourceTable(tableName, project);

        // remove streaming info
        SourceManager sourceManager = SourceManager.getInstance(KylinConfig.getInstanceFromEnv());
        ISource source = sourceManager.getCachedSource(desc);
        if (!desc.isStreamingTable() || needRemoveStreamInfo) {
            source.unloadTable(tableName, project);
        }
        return rtn;
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
        List<String> hiveTableNames = explr.listTables(database);
        Iterable<String> kylinApplicationTableNames = Iterables.filter(hiveTableNames, new Predicate<String>() {
            @Override
            public boolean apply(@Nullable String input) {
                return input != null && !input.startsWith(getConfig().getHiveIntermediateTablePrefix());
            }
        });
        return Lists.newArrayList(kylinApplicationTableNames);
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
        // calculate cardinality for Hive source
        ProjectInstance projectInstance = getProjectManager().getProject(prj);
        if (projectInstance == null || projectInstance.getSourceType() != ISourceAware.ID_HIVE){
            return;
        }
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

    public void updateSnapshotLocalCache(String project, String tableName, String snapshotID) {
        ExtTableSnapshotInfoManager snapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(getConfig());
        ExtTableSnapshotInfo extTableSnapshotInfo = snapshotInfoManager.getSnapshot(tableName, snapshotID);
        TableDesc tableDesc = getTableManager().getTableDesc(tableName, project);
        if (extTableSnapshotInfo == null) {
            throw new IllegalArgumentException(
                    "cannot find ext snapshot info for table:" + tableName + " snapshot:" + snapshotID);
        }
        LookupProviderFactory.rebuildLocalCache(tableDesc, extTableSnapshotInfo);
    }

    public void removeSnapshotLocalCache(String tableName, String snapshotID) {
        ExtTableSnapshotInfoManager snapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(getConfig());
        ExtTableSnapshotInfo extTableSnapshotInfo = snapshotInfoManager.getSnapshot(tableName, snapshotID);
        if (extTableSnapshotInfo == null) {
            throw new IllegalArgumentException(
                    "cannot find ext snapshot info for table:" + tableName + " snapshot:" + snapshotID);
        }
        LookupProviderFactory.removeLocalCache(extTableSnapshotInfo);
    }

    public String getSnapshotLocalCacheState(String tableName, String snapshotID) {
        ExtTableSnapshotInfoManager snapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(getConfig());
        ExtTableSnapshotInfo extTableSnapshotInfo = snapshotInfoManager.getSnapshot(tableName, snapshotID);
        if (extTableSnapshotInfo == null) {
            throw new IllegalArgumentException(
                    "cannot find ext snapshot info for table:" + tableName + " snapshot:" + snapshotID);
        }
        CacheState cacheState = LookupProviderFactory.getCacheState(extTableSnapshotInfo);
        return cacheState.name();
    }

    public List<TableSnapshotResponse> getLookupTableSnapshots(String project, String tableName) throws IOException {
        TableDesc tableDesc = getTableManager().getTableDesc(tableName, project);
        IReadableTable hiveTable = SourceManager.createReadableTable(tableDesc, null);
        TableSignature signature = hiveTable.getSignature();
        return internalGetLookupTableSnapshots(tableName, signature);
    }

    List<TableSnapshotResponse> internalGetLookupTableSnapshots(String tableName, TableSignature signature)
            throws IOException {
        ExtTableSnapshotInfoManager extSnapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(getConfig());
        SnapshotManager snapshotManager = SnapshotManager.getInstance(getConfig());
        List<ExtTableSnapshotInfo> extTableSnapshots = extSnapshotInfoManager.getSnapshots(tableName);
        List<SnapshotTable> metaStoreTableSnapshots = snapshotManager.getSnapshots(tableName, signature);

        Map<String, List<String>> snapshotUsageMap = getSnapshotUsages();

        List<TableSnapshotResponse> result = Lists.newArrayList();
        for (ExtTableSnapshotInfo extTableSnapshot : extTableSnapshots) {
            TableSnapshotResponse response = new TableSnapshotResponse();
            response.setSnapshotID(extTableSnapshot.getId());
            response.setSnapshotType(TableSnapshotResponse.TYPE_EXT);
            response.setLastBuildTime(extTableSnapshot.getLastBuildTime());
            response.setStorageType(extTableSnapshot.getStorageType());
            response.setSourceTableSize(extTableSnapshot.getSignature().getSize());
            response.setSourceTableLastModifyTime(extTableSnapshot.getSignature().getLastModifiedTime());
            response.setCubesAndSegmentsUsage(snapshotUsageMap.get(extTableSnapshot.getResourcePath()));
            result.add(response);
        }

        for (SnapshotTable metaStoreTableSnapshot : metaStoreTableSnapshots) {
            TableSnapshotResponse response = new TableSnapshotResponse();
            response.setSnapshotID(metaStoreTableSnapshot.getId());
            response.setSnapshotType(TableSnapshotResponse.TYPE_INNER);
            response.setLastBuildTime(metaStoreTableSnapshot.getLastBuildTime());
            response.setStorageType(SnapshotTable.STORAGE_TYPE_METASTORE);
            response.setSourceTableSize(metaStoreTableSnapshot.getSignature().getSize());
            response.setSourceTableLastModifyTime(metaStoreTableSnapshot.getSignature().getLastModifiedTime());
            response.setCubesAndSegmentsUsage(snapshotUsageMap.get(metaStoreTableSnapshot.getResourcePath()));
            result.add(response);
        }

        return result;
    }

    /**
     * @return Map of SnapshotID, CubeName or SegmentName list
     */
    private Map<String, List<String>> getSnapshotUsages() {
        CubeManager cubeManager = CubeManager.getInstance(getConfig());
        Map<String, List<String>> snapshotCubeSegmentMap = Maps.newHashMap();
        for (CubeInstance cube : cubeManager.listAllCubes()) {
            Collection<String> cubeSnapshots = cube.getSnapshots().values();
            for (String cubeSnapshot : cubeSnapshots) {
                List<String> usages = snapshotCubeSegmentMap.get(cubeSnapshot);
                if (usages == null) {
                    usages = Lists.newArrayList();
                    snapshotCubeSegmentMap.put(cubeSnapshot, usages);
                }
                usages.add(cube.getName());
            }
            for (CubeSegment segment : cube.getSegments()) {
                Collection<String> segmentSnapshots = segment.getSnapshotPaths();
                for (String segmentSnapshot : segmentSnapshots) {
                    List<String> usages = snapshotCubeSegmentMap.get(segmentSnapshot);
                    if (usages == null) {
                        usages = Lists.newArrayList();
                        snapshotCubeSegmentMap.put(segmentSnapshot, usages);
                    }
                    usages.add(cube.getName() + ":" + segment.getName());
                }
            }
        }
        return snapshotCubeSegmentMap;
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
            BadRequestException e = new BadRequestException(
                    String.format(Locale.ROOT, msg.getTABLE_DESC_NOT_FOUND(), tableName));
            logger.error("Cannot find table descriptor " + tableName, e);
            throw e;
        }

        CardinalityExecutable job = new CardinalityExecutable();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        //make sure the job could be scheduled when the DistributedScheduler is enable.
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss", Locale.ROOT);
        format.setTimeZone(TimeZone.getTimeZone(kylinConfig.getTimeZone()));
        job.setParam("segmentId", tableName);
        job.setProjectName(prj);
        job.setName("Hive Column Cardinality - " + tableName + " - "
            + format.format(new Date(System.currentTimeMillis())));
        job.setSubmitter(submitter);

        String outPath = getConfig().getHdfsWorkingDirectory() + "cardinality/" + job.getId() + "/" + tableName;
        String param = "-table " + tableName + " -output " + outPath + " -project " + prj;

        if (getConfig().isSparkCardinalityEnabled()) { // use spark engine to calculate cardinality
            SparkExecutable step1 = new SparkExecutable();
            step1.setClassName(SparkColumnCardinality.class.getName());
            step1.setParam(SparkColumnCardinality.OPTION_OUTPUT.getOpt(), outPath);
            step1.setParam(SparkColumnCardinality.OPTION_PRJ.getOpt(), prj);
            step1.setParam(SparkColumnCardinality.OPTION_TABLE_NAME.getOpt(), tableName);
            step1.setParam(SparkColumnCardinality.OPTION_COLUMN_COUNT.getOpt(), String.valueOf(table.getColumnCount()));
            step1.setName(STEP_NAME_CALCULATE_COLUMN_CARDINALITY);
            job.addTask(step1);
        } else {
            MapReduceExecutable step1 = new MapReduceExecutable();
            step1.setMapReduceJobClass(HiveColumnCardinalityJob.class);
            step1.setMapReduceParams(param);
            step1.setParam("segmentId", tableName);
            step1.setName(STEP_NAME_CALCULATE_COLUMN_CARDINALITY);
            job.addTask(step1);
        }

        HadoopShellExecutable step2 = new HadoopShellExecutable();

        step2.setJobClass(HiveColumnCardinalityUpdateJob.class);
        step2.setJobParams(param);
        step2.setParam("segmentId", tableName);
        step2.setName(STEP_NAME_SAVE_CARDINALITY_TO_METADATA);
        job.addTask(step2);
        tableExt.setJodID(job.getId());
        getTableManager().saveTableExt(tableExt, prj);

        getExecutableManager().addJob(job);
    }

    public String normalizeHiveTableName(String tableName) {
        String[] dbTableName = HadoopUtil.parseHiveTableName(tableName);
        return (dbTableName[0] + "." + dbTableName[1]).toUpperCase(Locale.ROOT);
    }

    /**
     * 1. Check whether it's able to do the change
     *      - related cube instance should be disabled
     * 2. Get all influenced metadata
     *      - table
     *      - project
     *      - model
     *      - cube desc
     *      - cube instance
     * 3. Update the metadata
     * 4. Save the updated metadata
     *      - table
     *      - project
     *      - model
     *      - cube desc
     *      - cube instance
     * 5. Delete dirty table metadata
     */
    public void updateHiveTable(String projectName, Map<String, TableSchemaUpdateMapping> mapping, boolean isUseExisting) throws IOException {
        final ProjectInstance prjInstance = getProjectManager().getProject(projectName);
        if (prjInstance == null) {
            throw new BadRequestException("Project " + projectName + " does not exist");
        }
        // To deal with case sensitive issue for table resource path
        final String project = prjInstance.getName();
        aclEvaluate.checkProjectWritePermission(project);

        // Check whether it's able to do the change
        Set<CubeInstance> infCubes = cubeService.listAllCubes(project).stream()
                .filter(cube -> isTablesUsed(cube.getModel(), mapping.keySet())).collect(Collectors.toSet());
        Set<CubeInstance> readyCubeSet = infCubes.stream().filter(cube -> cube.isReady()).collect(Collectors.toSet());
        if (!readyCubeSet.isEmpty()) {
            throw new BadRequestException("Influenced cubes " + readyCubeSet + " should be disabled");
        }

        // Get influenced metadata and update the metadata
        Map<String, TableDesc> newTables = mapping.keySet().stream()
                .map(t -> getTableManager().getTableDesc(t, project)).collect(Collectors.toMap(t -> t.getIdentity(),
                        t -> TableSchemaUpdater.dealWithMappingForTable(t, mapping)));
        Map<String, String> existingTables = newTables.entrySet().stream()
                .filter(t -> getTableManager().getTableDesc(t.getValue().getIdentity(), project, false) != null)
                .collect(Collectors.toMap(t -> t.getKey(), t -> t.getValue().getIdentity()));
        if (!existingTables.isEmpty()) {
            if (isUseExisting) {
                logger.info("Will use existing tables {}", existingTables.values());
            } else {
                throw new BadRequestException("Tables " + existingTables.values() + " already exist");
            }
        }
        Map<String, DataModelDesc> newModels = prjInstance.getModels().stream()
                .map(m -> getDataModelManager().getDataModelDesc(m)).filter(m -> isTablesUsed(m, mapping.keySet()))
                .map(m -> TableSchemaUpdater.dealWithMappingForModel(m, mapping))
                .collect(Collectors.toMap(m -> m.getName(), m -> m));

        Map<String, CubeDesc> newCubeDescs = infCubes.stream()
                .map(cube -> TableSchemaUpdater.dealWithMappingForCubeDesc(cube.getDescriptor(), mapping))
                .collect(Collectors.toMap(cube -> cube.getName(), cube -> cube));
        Map<String, CubeInstance> newCubes = infCubes.stream()
                .map(cube -> TableSchemaUpdater.dealWithMappingForCube(cube, mapping))
                .collect(Collectors.toMap(cube -> cube.getName(), cube -> cube));

        // Save the updated metadata
        // -- 1. table & table_ext
        for (Map.Entry<String, TableDesc> entry : newTables.entrySet()) {
            if (existingTables.containsKey(entry.getKey())) {
                continue;
            }
            getTableManager().saveNewTableExtFromOld(entry.getKey(), project, entry.getValue().getIdentity());
            getTableManager().saveSourceTable(entry.getValue(), project);
        }
        // -- 2. project
        Set<String> newTableNames = newTables.values().stream().map(t -> t.getIdentity()).collect(Collectors.toSet());
        getProjectManager().addTableDescToProject(newTableNames.toArray(new String[0]), project);
        // -- 3. model
        for (Map.Entry<String, DataModelDesc> entry : newModels.entrySet()) {
            getDataModelManager().updateDataModelDesc(entry.getValue());
        }
        // -- 4. cube_desc & cube instance
        for (Map.Entry<String, CubeDesc> entry : newCubeDescs.entrySet()) {
            getCubeDescManager().updateCubeDesc(entry.getValue());
        }
        for (Map.Entry<String, CubeInstance> entry : newCubes.entrySet()) {
            CubeUpdate update = new CubeUpdate(entry.getValue());
            getCubeManager().updateCube(update);
        }

        // Delete dirty table metadata
        Set<String> oldTables = Sets.newHashSet(newTables.keySet());
        oldTables.removeAll(existingTables.values());
        getProjectManager().removeTableDescFromProject(oldTables.toArray(new String[0]), project);
        for (String entry : newTables.keySet()) {
            getTableManager().removeTableExt(entry, project);
            getTableManager().removeSourceTable(entry, project);
        }
    }

    private static boolean isTablesUsed(DataModelDesc model, Set<String> tables) {
        Set<String> usingTables = model.getAllTables().stream().map(t -> t.getTableIdentity())
                .collect(Collectors.toSet());
        usingTables.retainAll(tables);
        return !usingTables.isEmpty();
    }
}
