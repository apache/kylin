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

package org.apache.kylin.tool;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 12/29/15.
 */
public class ExtendCubeToHybridCLI {
    public static final String ACL_INFO_FAMILY = "i";
    private static final String CUBE_POSTFIX = "_old";
    private static final String HYBRID_POSTFIX = "_hybrid";
    private static final Logger logger = LoggerFactory.getLogger(ExtendCubeToHybridCLI.class);
    private static final String ACL_INFO_FAMILY_PARENT_COLUMN = "p";

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private MetadataManager metadataManager;
    private ResourceStore store;

    public ExtendCubeToHybridCLI() {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.store = ResourceStore.getStore(kylinConfig);
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        this.metadataManager = MetadataManager.getInstance(kylinConfig);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 3) {
            System.out.println("Usage: ExtendCubeToHybridCLI project cube [partition_date]");
            return;
        }

        ExtendCubeToHybridCLI tool = new ExtendCubeToHybridCLI();

        String projectName = args[0];
        String cubeName = args[1];
        String partitionDate = args.length == 3 ? args[2] : null;

        try {
            tool.createFromCube(projectName, cubeName, partitionDate);
            tool.verify();
            logger.info("Job Finished.");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Job Aborted.", e.getMessage());
        }
    }

    private boolean validateCubeInstance(CubeInstance cubeInstance) {
        if (cubeInstance == null) {
            logger.error("This cube does not exist.");
            return false;
        }
        if (cubeInstance.getSegments().isEmpty()) {
            logger.error("No segments in this cube, no need to extend.");
            return false;
        }
        return true;
    }

    public void createFromCube(String projectName, String cubeName, String partitionDateStr) throws Exception {
        logger.info("Create hybrid for cube[" + cubeName + "], project[" + projectName + "], partition_date[" + partitionDateStr + "].");

        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        if (!validateCubeInstance(cubeInstance)) {
            return;
        }

        CubeDesc cubeDesc = cubeDescManager.getCubeDesc(cubeInstance.getDescName());
        DataModelDesc dataModelDesc = metadataManager.getDataModelDesc(cubeDesc.getModelName());
        if (StringUtils.isEmpty(dataModelDesc.getPartitionDesc().getPartitionDateColumn())) {
            logger.error("No incremental cube, no need to extend.");
            return;
        }

        String owner = cubeInstance.getOwner();
        long partitionDate = partitionDateStr != null ? DateFormat.stringToMillis(partitionDateStr) : 0;

        // get new name for old cube and cube_desc
        String newCubeDescName = renameCube(cubeDesc.getName());
        String newCubeInstanceName = renameCube(cubeInstance.getName());
        while (cubeDescManager.getCubeDesc(newCubeDescName) != null)
            newCubeDescName = renameCube(newCubeDescName);
        while (cubeManager.getCube(newCubeInstanceName) != null)
            newCubeInstanceName = renameCube(newCubeInstanceName);

        // create new cube_instance for old segments
        CubeInstance newCubeInstance = CubeInstance.getCopyOf(cubeInstance);
        newCubeInstance.setName(newCubeInstanceName);
        newCubeInstance.setDescName(newCubeDescName);
        newCubeInstance.updateRandomUuid();
        Iterator<CubeSegment> segmentIterator = newCubeInstance.getSegments().iterator();
        CubeSegment currentSeg = null;
        while (segmentIterator.hasNext()) {
            currentSeg = segmentIterator.next();
            if (partitionDateStr != null && (currentSeg.getDateRangeStart() >= partitionDate || currentSeg.getDateRangeEnd() > partitionDate)) {
                segmentIterator.remove();
                logger.info("CubeSegment[" + currentSeg + "] was removed.");
            }
        }
        if (currentSeg != null && partitionDateStr != null && partitionDate != currentSeg.getDateRangeEnd()) {
            logger.error("PartitionDate must be end date of one segment.");
            return;
        }
        if (currentSeg != null && partitionDateStr == null)
            partitionDate = currentSeg.getDateRangeEnd();

        cubeManager.createCube(newCubeInstance, projectName, owner);
        logger.info("CubeInstance was saved at: " + newCubeInstance.getResourcePath());

        // create new cube for old segments
        CubeDesc newCubeDesc = CubeDesc.getCopyOf(cubeDesc);
        newCubeDesc.setName(newCubeDescName);
        newCubeDesc.updateRandomUuid();
        newCubeDesc.init(kylinConfig);
        newCubeDesc.setPartitionDateEnd(partitionDate);
        newCubeDesc.calculateSignature();
        cubeDescManager.createCubeDesc(newCubeDesc);
        logger.info("CubeDesc was saved at: " + newCubeDesc.getResourcePath());

        // update old cube_desc to new-version metadata
        cubeDesc.setPartitionDateStart(partitionDate);
        cubeDesc.setEngineType(IEngineAware.ID_MR_V2);
        cubeDesc.setStorageType(IStorageAware.ID_SHARDED_HBASE);
        cubeDesc.calculateSignature();
        cubeDescManager.updateCubeDesc(cubeDesc);
        logger.info("CubeDesc was saved at: " + cubeDesc.getResourcePath());

        // clear segments for old cube
        cubeInstance.setSegments(new Segments());
        cubeInstance.setStatus(RealizationStatusEnum.DISABLED);
        store.putResource(cubeInstance.getResourcePath(), cubeInstance, CubeManager.CUBE_SERIALIZER);
        logger.info("CubeInstance was saved at: " + cubeInstance.getResourcePath());

        // create hybrid model for these two cubes
        List<RealizationEntry> realizationEntries = Lists.newArrayListWithCapacity(2);
        realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, cubeInstance.getName()));
        realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, newCubeInstance.getName()));
        HybridInstance hybridInstance = HybridInstance.create(kylinConfig, renameHybrid(cubeInstance.getName()), realizationEntries);
        store.putResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
        ProjectManager.getInstance(kylinConfig).moveRealizationToProject(RealizationType.HYBRID, hybridInstance.getName(), projectName, owner);
        logger.info("HybridInstance was saved at: " + hybridInstance.getResourcePath());

        // copy Acl from old cube to new cube
        copyAcl(cubeInstance.getId(), newCubeInstance.getId(), projectName);
        logger.info("Acl copied from [" + cubeName + "] to [" + newCubeInstanceName + "].");
    }

    private void verify() {
        CubeDescManager.clearCache();
        CubeDescManager.getInstance(kylinConfig);

        CubeManager.clearCache();
        CubeManager.getInstance(kylinConfig);

        ProjectManager.clearCache();
        ProjectManager.getInstance(kylinConfig);

        HybridManager.clearCache();
        HybridManager.getInstance(kylinConfig);
    }

    private String renameCube(String origName) {
        return origName + CUBE_POSTFIX;
    }

    private String renameHybrid(String origName) {
        return origName + HYBRID_POSTFIX;
    }

    private void copyAcl(String origCubeId, String newCubeId, String projectName) throws Exception {
        String projectResPath = ProjectInstance.concatResourcePath(projectName);
        Serializer<ProjectInstance> projectSerializer = new JsonSerializer<ProjectInstance>(ProjectInstance.class);
        ProjectInstance project = store.getResource(projectResPath, ProjectInstance.class, projectSerializer);
        String projUUID = project.getUuid();
        Table aclHtable = null;
        try {
            aclHtable = HBaseConnection.get(kylinConfig.getStorageUrl()).getTable(TableName.valueOf(kylinConfig.getMetadataUrlPrefix() + "_acl"));

            // cube acl
            Result result = aclHtable.get(new Get(Bytes.toBytes(origCubeId)));
            if (result.listCells() != null) {
                for (Cell cell : result.listCells()) {
                    byte[] family = CellUtil.cloneFamily(cell);
                    byte[] column = CellUtil.cloneQualifier(cell);
                    byte[] value = CellUtil.cloneValue(cell);

                    // use the target project uuid as the parent
                    if (Bytes.toString(family).equals(ACL_INFO_FAMILY) && Bytes.toString(column).equals(ACL_INFO_FAMILY_PARENT_COLUMN)) {
                        String valueString = "{\"id\":\"" + projUUID + "\",\"type\":\"org.apache.kylin.metadata.project.ProjectInstance\"}";
                        value = Bytes.toBytes(valueString);
                    }
                    Put put = new Put(Bytes.toBytes(newCubeId));
                    put.add(family, column, value);
                    aclHtable.put(put);
                }
            }
        } finally {
            IOUtils.closeQuietly(aclHtable);
        }
    }
}
