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

package org.apache.kylin.tool.migration;

import static org.apache.kylin.metadata.realization.IRealizationConstants.HTableSegmentTag;

import java.io.IOException;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.dict.lookup.SnapshotTableSerializer;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class DstClusterUtil extends ClusterUtil {
    private static final Logger logger = LoggerFactory.getLogger(DstClusterUtil.class);

    public static final String hbaseSubDir = "migration/hbase/data/default/";

    private final String hbaseDataDirQualified;
    private final String hbaseDataDir;

    private final boolean ifExecute;

    public DstClusterUtil(String configURI, boolean ifExecute) throws IOException {
        this(configURI, true, true, ifExecute);
    }

    public DstClusterUtil(String configURI, boolean ifJobFSHAEnabled, boolean ifHBaseFSHAEnabled, boolean ifExecute)
            throws IOException {
        super(configURI, ifJobFSHAEnabled, ifHBaseFSHAEnabled);
        this.hbaseDataDirQualified = hbaseHdfsWorkingDirectoryQualified + hbaseSubDir;
        this.hbaseDataDir = hdfsWorkingDirectory + hbaseSubDir;
        this.ifExecute = ifExecute;
    }

    @Override
    public ProjectInstance getProject(String projName) throws IOException {
        return resourceStore.getResource(ProjectInstance.concatResourcePath(projName),
                ProjectManager.PROJECT_SERIALIZER);
    }

    @Override
    public DictionaryInfo getDictionaryInfo(String dictPath) throws IOException {
        return resourceStore.getResource(dictPath, DictionaryInfoSerializer.FULL_SERIALIZER);
    }

    @Override
    public SnapshotTable getSnapshotTable(String snapshotPath) throws IOException {
        return resourceStore.getResource(snapshotPath, SnapshotTableSerializer.FULL_SERIALIZER);
    }

    @Override
    public String getRootDirQualifiedOfHTable(String tableName) {
        return hbaseDataDirQualified + tableName;
    }

    private String getRootDirOfHTable(String tableName) {
        return hbaseDataDir + tableName;
    }

    public boolean exists(String resPath) throws IOException {
        return resourceStore.exists(resPath);
    }

    public void checkCompatibility(String projectName, Set<TableDesc> tableSet, DataModelDesc modelDesc,
            boolean ifHiveCheck) throws IOException {
        List<String> tableDataList = Lists.newArrayList();
        for (TableDesc table : tableSet) {
            tableDataList.add(JsonUtil.writeValueAsIndentString(table));
        }

        String modelDescData = JsonUtil.writeValueAsIndentString(modelDesc);

        CompatibilityCheckRequest request = new CompatibilityCheckRequest();
        request.setProjectName(projectName);
        request.setTableDescDataList(tableDataList);
        request.setModelDescData(modelDescData);

        String jsonRequest = JsonUtil.writeValueAsIndentString(request);
        restClient.checkCompatibility(jsonRequest, ifHiveCheck);
    }

    public void saveProject(ProjectInstance projInstance) throws IOException {
        if (ifExecute) {
            putMetaResource(ProjectInstance.concatResourcePath(projInstance.getName()), projInstance,
                    ProjectManager.PROJECT_SERIALIZER);
        }
        logger.info("saved project {}", projInstance);
    }

    public void saveHybrid(HybridInstance hybridInstance) throws IOException {
        if (ifExecute) {
            putMetaResource(HybridInstance.concatResourcePath(hybridInstance.getName()), hybridInstance,
                    HybridManager.HYBRID_SERIALIZER);
        }
        logger.info("saved hybrid {}", hybridInstance);
    }

    public void saveTableDesc(TableDesc table) throws IOException {
        if (ifExecute) {
            putMetaResource(TableDesc.concatResourcePath(table.getIdentity(), table.getProject()), table,
                    TableMetadataManager.TABLE_SERIALIZER);
        }
        logger.info("saved table {}", table);
    }

    public void saveModelDesc(DataModelDesc modelDesc) throws IOException {
        if (ifExecute) {
            putMetaResource(DataModelDesc.concatResourcePath(modelDesc.getName()), modelDesc,
                    DataModelManager.MODELDESC_SERIALIZER);
        }
        logger.info("saved model {}", modelDesc);
    }

    public void saveCubeDesc(CubeDesc cubeDesc) throws IOException {
        if (ifExecute) {
            putMetaResource(CubeDesc.concatResourcePath(cubeDesc.getName()), cubeDesc,
                    CubeDescManager.CUBE_DESC_SERIALIZER);
        }
        logger.info("saved cube desc {}", cubeDesc);
    }

    public void saveCubeInstance(CubeInstance cube) throws IOException {
        if (ifExecute) {
            putMetaResource(CubeInstance.concatResourcePath(cube.getName()), cube, CubeManager.CUBE_SERIALIZER);
        }
        logger.info("saved cube instance {}", cube);
    }

    public String saveDictionary(DictionaryInfo dictInfo) throws IOException {
        String dupDict = checkDupDict(dictInfo);
        if (dupDict == null) {
            putMetaResource(dictInfo.getResourcePath(), dictInfo, DictionaryInfoSerializer.FULL_SERIALIZER);
            logger.info("saved dictionary {}", dictInfo.getResourcePath());
        }
        return dupDict;
    }

    private String checkDupDict(DictionaryInfo dictInfo) throws IOException {
        NavigableSet<String> existings = resourceStore.listResources(dictInfo.getResourceDir());
        if (existings == null)
            return null;

        logger.info("{} existing dictionaries of the same column", existings.size());
        if (existings.size() > 100) {
            logger.warn("Too many dictionaries under {}, dict count: {}", dictInfo.getResourceDir(), existings.size());
        }

        for (String existing : existings) {
            DictionaryInfo existingInfo = getDictionaryInfo(existing);
            if (existingInfo != null && dictInfo.getDictionaryObject().equals(existingInfo.getDictionaryObject())) {
                return existing;
            }
        }

        return null;
    }

    public void saveSnapshotTable(SnapshotTable snapshotTable) throws IOException {
        putMetaResource(snapshotTable.getResourcePath(), snapshotTable, SnapshotTableSerializer.FULL_SERIALIZER);
        logger.info("saved snapshot table {}", snapshotTable.getResourcePath());
    }

    public void saveExtSnapshotTableInfo(ExtTableSnapshotInfo extTableSnapshotInfo) throws IOException {
        putMetaResource(extTableSnapshotInfo.getResourcePath(), extTableSnapshotInfo,
                ExtTableSnapshotInfoManager.SNAPSHOT_SERIALIZER);
        logger.info("saved ext snapshot table info {}", extTableSnapshotInfo.getResourcePath());
    }

    public void saveUserInfo(String userKey, ManagedUser user) throws IOException {
        if (ifExecute) {
            putMetaResource(userKey, user, KylinUserService.SERIALIZER);
        }
        logger.info("saved user info {}", userKey);
    }

    private <T extends RootPersistentEntity> void putMetaResource(String resPath, T obj, Serializer<T> serializer)
            throws IOException {
        putMetaResource(resPath, obj, serializer, true);
    }

    public <T extends RootPersistentEntity> void putMetaResource(String resPath, T obj, Serializer<T> serializer,
            boolean withoutCheck) throws IOException {
        if (ifExecute) {
            if (withoutCheck) {
                resourceStore.putResource(resPath, obj, System.currentTimeMillis(), serializer);
            } else {
                resourceStore.checkAndPutResource(resPath, obj, System.currentTimeMillis(), serializer);
            }
        }
        logger.info("saved resource {}", resPath);
    }

    public void putResource(String resPath, RawResource res) throws IOException {
        if (ifExecute) {
            resourceStore.putResource(resPath, res.content(), res.lastModified());
        }
        logger.info("saved resource {}", resPath);
    }

    // if htable does not exist in dst, return false;
    // if htable exists in dst, and the segment tags are the same, if the htable is enabled, then return true;
    //                                                             else delete the htable and return false;
    //                          else the htable is used by others, should throw runtime exception
    public boolean checkExist(TableName htableName, CubeSegment segment) throws IOException {
        if (!htableExists(htableName)) {
            return false;
        }
        Table table = hbaseConn.getTable(htableName);
        HTableDescriptor tableDesc = table.getTableDescriptor();
        if (segment.toString().equals(tableDesc.getValue(HTableSegmentTag))) {
            if (hbaseAdmin.isTableEnabled(htableName)) {
                return true;
            } else {
                hbaseAdmin.deleteTable(htableName);
                logger.info("htable {} is deleted", htableName);
                return false;
            }
        }
        throw new RuntimeException(
                "htable name " + htableName + " has been used by " + tableDesc.getValue(HTableSegmentTag));
    }

    public void deleteHTable(String tableName) throws IOException {
        TableName htableName = TableName.valueOf(tableName);
        if (hbaseAdmin.isTableEnabled(htableName)) {
            hbaseAdmin.disableTable(htableName);
        }
        hbaseAdmin.deleteTable(htableName);
        logger.info("htable {} is deleted", htableName);
    }

    public boolean htableExists(TableName htableName) throws IOException {
        return hbaseAdmin.tableExists(htableName);
    }

    public void resetTableHost(HTableDescriptor tableDesc) {
        tableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());
    }

    public void deployCoprocessor(HTableDescriptor tableDesc, String localCoprocessorJar) throws IOException {
        List<String> existingCoprocessors = tableDesc.getCoprocessors();
        for (String existingCoprocessor : existingCoprocessors) {
            tableDesc.removeCoprocessor(existingCoprocessor);
        }

        Path hdfsCoprocessorJar = DeployCoprocessorCLI.uploadCoprocessorJar(localCoprocessorJar, hbaseFS,
                hdfsWorkingDirectory, null);

        if (User.isHBaseSecurityEnabled(hbaseConf)) {
            // add coprocessor for bulk load
            tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
        }
        DeployCoprocessorCLI.addCoprocessorOnHTable(tableDesc, hdfsCoprocessorJar);

        logger.info("deployed hbase table {} with coprocessor.", tableDesc.getTableName());
    }

    public void createTable(HTableDescriptor tableDesc, byte[][] splitKeys) throws IOException {
        hbaseAdmin.createTable(tableDesc, splitKeys);

        logger.info("htable {} successfully created!", tableDesc.getTableName());
    }

    public void copyInitOnJobCluster(Path path) throws IOException {
        copyInit(jobFS, path);
    }

    public void copyInitOnHBaseCluster(Path path) throws IOException {
        copyInit(hbaseFS, path);
    }

    public static void copyInit(FileSystem fs, Path path) throws IOException {
        path = Path.getPathWithoutSchemeAndAuthority(path);
        Path pathP = path.getParent();
        if (!fs.exists(pathP)) {
            fs.mkdirs(pathP);
        }
        if (fs.exists(path)) {
            logger.warn("path {} already existed and will be deleted", path);
            HadoopUtil.deletePath(fs.getConf(), path);
        }
    }

    public void bulkLoadTable(String tableName) throws Exception {
        Path rootPathOfTable = new Path(getRootDirOfHTable(tableName));
        FileStatus[] regionFiles = hbaseFS.listStatus(rootPathOfTable, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return !path.getName().startsWith(".");
            }
        });

        for (FileStatus regionFileStatus : regionFiles) {
            ToolRunner.run(new LoadIncrementalHFiles(hbaseConf),
                    new String[] { regionFileStatus.getPath().toString(), tableName });
        }

        logger.info("succeed to migrate htable {}", tableName);
    }

    public void updateMeta() {
        if (ifExecute) {
            try {
                logger.info("update meta cache for {}", restClient);
                restClient.announceWipeCache(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE.getType(),
                        Broadcaster.SYNC_ALL);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
