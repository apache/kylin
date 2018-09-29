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

package org.apache.kylin.storage.parquet.cube;

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStorage;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.parquet.spark.ParquetPayload;
import org.apache.kylin.storage.parquet.spark.SparkSubmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class CubeSparkRPC implements IGTStorage {

    public static final Logger logger = LoggerFactory.getLogger(CubeSparkRPC.class);

    protected CubeSegment cubeSegment;
    protected Cuboid cuboid;
    protected GTInfo gtInfo;
    protected StorageContext storageContext;

    public CubeSparkRPC(ISegment segment, Cuboid cuboid, GTInfo gtInfo, StorageContext context) {
        this.cubeSegment = (CubeSegment) segment;
        this.cuboid = cuboid;
        this.gtInfo = gtInfo;
        this.storageContext = context;
    }

    protected List<Integer> getRequiredParquetColumns(GTScanRequest request) {
        List<Integer> columnFamilies = Lists.newArrayList();

        for (int i = 0; i < request.getSelectedColBlocks().trueBitCount(); i++) {
            columnFamilies.add(request.getSelectedColBlocks().trueBitAt(i));
        }

        return columnFamilies;
    }

    @Override
    public IGTScanner getGTScanner(GTScanRequest scanRequest) throws IOException {
        String scanReqId = Integer.toHexString(System.identityHashCode(scanRequest));

        ParquetPayload.ParquetPayloadBuilder builder = new ParquetPayload.ParquetPayloadBuilder();

        JobBuilderSupport jobBuilderSupport = new JobBuilderSupport(cubeSegment, "");

<<<<<<< HEAD
=======
        String cubooidRootPath = jobBuilderSupport.getCuboidRootPath();

>>>>>>> 198041d63... KYLIN-3625 Init query
        List<List<Long>> layeredCuboids = cubeSegment.getCuboidScheduler().getCuboidsByLayer();
        int level = 0;
        for (List<Long> levelCuboids : layeredCuboids) {
            if (levelCuboids.contains(cuboid.getId())) {
                 break;
            }
            level++;
        }

<<<<<<< HEAD
        String dataFolderName;
        String parquetRootPath = jobBuilderSupport.getParquetOutputPath();
        dataFolderName = JobBuilderSupport.getCuboidOutputPathsByLevel(parquetRootPath, level) + "/" + cuboid.getId();
=======
        String dataFolderName = JobBuilderSupport.getCuboidOutputPathsByLevel(cubooidRootPath, level) + "/" + cuboid.getId();
>>>>>>> 198041d63... KYLIN-3625 Init query

        builder.setGtScanRequest(scanRequest.toByteArray()).setGtScanRequestId(scanReqId)
                .setKylinProperties(KylinConfig.getInstanceFromEnv().exportAllToString())
                .setRealizationId(cubeSegment.getCubeInstance().getName()).setSegmentId(cubeSegment.getUuid())
                .setDataFolderName(dataFolderName)
                .setMaxRecordLength(scanRequest.getInfo().getMaxLength())
                .setParquetColumns(getRequiredParquetColumns(scanRequest))
                .setRealizationType(RealizationType.CUBE.toString()).setQueryId(QueryContext.current().getQueryId())
                .setSpillEnabled(cubeSegment.getConfig().getQueryCoprocessorSpillEnabled())
                .setMaxScanBytes(cubeSegment.getConfig().getPartitionMaxScanBytes())
                .setStartTime(scanRequest.getStartTime()).setStorageType(cubeSegment.getStorageType());

        ParquetPayload payload = builder.createParquetPayload();

        logger.info("The scan {} for segment {} is ready to be submitted to spark client", scanReqId, cubeSegment);

        return SparkSubmitter.submitParquetTask(scanRequest, payload);
    }

}
