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

package org.apache.kylin.storage.hybrid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
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
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Created by dongli on 12/29/15.
 */
public class ExtendCubeToHybridTool {
    private static final Logger logger = LoggerFactory.getLogger(ExtendCubeToHybridTool.class);

    private KylinConfig kylinConfig;
    private CubeManager cubeManager;
    private CubeDescManager cubeDescManager;
    private MetadataManager metadataManager;
    private ResourceStore store;

    public ExtendCubeToHybridTool() {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.store = ResourceStore.getStore(kylinConfig);
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        this.metadataManager = MetadataManager.getInstance(kylinConfig);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 3) {
            System.out.println("Usage: ExtendCubeToHybridTool project cube [partition_date]");
            return;
        }

        ExtendCubeToHybridTool tool = new ExtendCubeToHybridTool();

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

    public void createFromCube(String projectName, String cubeName, String partitionDateStr) throws IOException {
        logger.info("Create hybrid for cube[" + cubeName + "], project[" + projectName + "], partition_date[" + partitionDateStr + "].");

        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        if (!validateCubeInstance(cubeInstance)) {
            return;
        }

        CubeDesc cubeDesc = cubeDescManager.getCubeDesc(cubeInstance.getDescName());
        DataModelDesc dataModelDesc = metadataManager.getDataModelDesc(cubeDesc.getModelName());
        String owner = cubeInstance.getOwner();
        String dateFormat = dataModelDesc.getPartitionDesc().getPartitionDateFormat();
        long partitionDate = partitionDateStr != null ? DateFormat.stringToMillis(partitionDateStr, dateFormat) : 0;

        // get new name for old cube and cube_desc
        String newCubeDescName = rename(cubeDesc.getName());
        String newCubeInstanceName = rename(cubeInstance.getName());
        while (cubeDescManager.getCubeDesc(newCubeDescName) != null)
            newCubeDescName = rename(newCubeDescName);
        while (cubeManager.getCube(newCubeInstanceName) != null)
            newCubeInstanceName = rename(newCubeInstanceName);

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
        if (partitionDateStr != null && partitionDate != currentSeg.getDateRangeEnd()) {
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
        newCubeDesc.init(kylinConfig, metadataManager.getAllTablesMap());
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
        cubeInstance.setSegments(new ArrayList<CubeSegment>());
        store.putResource(cubeInstance.getResourcePath(), cubeInstance, CubeManager.CUBE_SERIALIZER);
        logger.info("CubeInstance was saved at: " + cubeInstance.getResourcePath());

        // create hybrid model for these two cubes
        List<RealizationEntry> realizationEntries = Lists.newArrayListWithCapacity(2);
        realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, cubeInstance.getName()));
        realizationEntries.add(RealizationEntry.create(RealizationType.CUBE, newCubeInstance.getName()));
        HybridInstance hybridInstance = HybridInstance.create(kylinConfig, cubeInstance.getName(), realizationEntries);
        store.putResource(hybridInstance.getResourcePath(), hybridInstance, HybridManager.HYBRID_SERIALIZER);
        ProjectManager.getInstance(kylinConfig).moveRealizationToProject(RealizationType.HYBRID, hybridInstance.getName(), projectName, owner);
        logger.info("HybridInstance was saved at: " + hybridInstance.getResourcePath());
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

    private String rename(String origName) {
        return origName + "_1";
    }
}
