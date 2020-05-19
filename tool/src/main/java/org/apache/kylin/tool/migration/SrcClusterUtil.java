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

import java.io.IOException;
import java.util.List;

import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationRegistry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SrcClusterUtil extends ClusterUtil {
    private static final Logger logger = LoggerFactory.getLogger(SrcClusterUtil.class);

    private static final String hbaseRootDirConfKey = "hbase.rootdir";
    private final String hbaseDataDir;

    private final TableMetadataManager metadataManager;
    private final DataModelManager modelManager;
    private final ProjectManager projectManager;
    private final HybridManager hybridManager;
    private final CubeManager cubeManager;
    private final CubeDescManager cubeDescManager;
    private final RealizationRegistry realizationRegistry;
    private final DictionaryManager dictionaryManager;
    private final SnapshotManager snapshotManager;
    private final ExtTableSnapshotInfoManager extSnapshotInfoManager;

    public SrcClusterUtil(String configURI, boolean ifJobFSHAEnabled, boolean ifHBaseFSHAEnabled) throws IOException {
        super(configURI, ifJobFSHAEnabled, ifHBaseFSHAEnabled);

        this.hbaseDataDir = hbaseConf.get(hbaseRootDirConfKey) + "/data/default/";
        metadataManager = TableMetadataManager.getInstance(kylinConfig);
        modelManager = DataModelManager.getInstance(kylinConfig);
        projectManager = ProjectManager.getInstance(kylinConfig);
        hybridManager = HybridManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);
        cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        realizationRegistry = RealizationRegistry.getInstance(kylinConfig);
        dictionaryManager = DictionaryManager.getInstance(kylinConfig);
        snapshotManager = SnapshotManager.getInstance(kylinConfig);
        extSnapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(kylinConfig);
    }

    public String getDefaultCoprocessorJarPath() {
        return kylinConfig.getCoprocessorLocalJar();
    }

    @Override
    public ProjectInstance getProject(String projectName) throws IOException {
        return projectManager.getProject(projectName);
    }

    public List<ProjectInstance> listAllProjects() throws IOException {
        return projectManager.listAllProjects();
    }

    public ProjectInstance getProjectByRealization(RealizationType type, String realizationName) throws IOException {
        List<ProjectInstance> ret = projectManager.findProjects(type, realizationName);
        return ret.isEmpty() ? null : ret.get(0);
    }

    public CubeInstance getCube(String name) throws IOException {
        return cubeManager.getCube(name);
    }

    public CubeDesc getCubeDesc(String name) throws IOException {
        return cubeDescManager.getCubeDesc(name);
    }

    public HybridInstance getHybrid(String name) throws IOException {
        return hybridManager.getHybridInstance(name);
    }

    public IRealization getRealization(RealizationEntry entry) throws IOException {
        return realizationRegistry.getRealization(entry.getType(), entry.getRealization());
    }

    public DataModelDesc getDataModelDesc(String modelName) throws IOException {
        return modelManager.getDataModelDesc(modelName);
    }

    public TableDesc getTableDesc(String tableIdentity, String projectName) throws IOException {
        TableDesc ret = metadataManager.getStore().getResource(TableDesc.concatResourcePath(tableIdentity, projectName),
                TableMetadataManager.TABLE_SERIALIZER);
        if (projectName != null && ret == null) {
            ret = metadataManager.getStore().getResource(TableDesc.concatResourcePath(tableIdentity, null),
                    TableMetadataManager.TABLE_SERIALIZER);
        }
        return ret;
    }

    @Override
    public DictionaryInfo getDictionaryInfo(String dictPath) throws IOException {
        return dictionaryManager.getDictionaryInfo(dictPath);
    }

    @Override
    public SnapshotTable getSnapshotTable(String snapshotPath) throws IOException {
        return snapshotManager.getSnapshotTable(snapshotPath);
    }

    public ExtTableSnapshotInfo getExtTableSnapshotInfo(String snapshotPath) throws IOException {
        return extSnapshotInfoManager.getSnapshot(snapshotPath);
    }

    @Override
    public String getRootDirQualifiedOfHTable(String tableName) {
        return hbaseDataDir + tableName;
    }
}