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

package org.apache.kylin.engine.spark;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.StorageFactory;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

public class SparkUtil {

    public static ISparkInput.ISparkBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        return SourceManager.createEngineAdapter(seg, ISparkInput.class).getBatchCubingInputSide(flatDesc);
    }

    public static ISparkOutput.ISparkBatchCubingOutputSide getBatchCubingOutputSide(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, ISparkOutput.class).getBatchCubingOutputSide(seg);
    }

    public static ISparkOutput.ISparkBatchMergeOutputSide getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, ISparkOutput.class).getBatchMergeOutputSide(seg);
    }

    public static ISparkInput.ISparkBatchMergeInputSide getBatchMergeInputSide(CubeSegment seg) {
        return SourceManager.createEngineAdapter(seg, ISparkInput.class).getBatchMergeInputSide(seg);
    }

    public static IMROutput2.IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchOptimizeOutputSide(seg);
    }

    /**
     * Read the given path as a Java RDD; The path can have second level sub folder.
     * @param inputPath
     * @param fs
     * @param sc
     * @param keyClass
     * @param valueClass
     * @return
     * @throws IOException
     */
    public static JavaPairRDD parseInputPath(String inputPath, FileSystem fs, JavaSparkContext sc, Class keyClass,
            Class valueClass) throws IOException {
        List<String> inputFolders = Lists.newArrayList();
        Path inputHDFSPath = new Path(inputPath);
        FileStatus[] fileStatuses = fs.listStatus(inputHDFSPath);
        boolean hasDir = false;
        for (FileStatus stat : fileStatuses) {
            if (stat.isDirectory() && !stat.getPath().getName().startsWith("_")) {
                hasDir = true;
                inputFolders.add(stat.getPath().toString());
            }
        }

        if (!hasDir) {
            return sc.sequenceFile(inputHDFSPath.toString(), keyClass, valueClass);
        }

        return sc.sequenceFile(StringUtil.join(inputFolders, ","), keyClass, valueClass);
    }


    public static int estimateLayerPartitionNum(int level, CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double baseCuboidSize = statsReader.estimateLayerSize(level);
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (baseCuboidSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        return partition;
    }


    public static int estimateTotalPartitionNum(CubeStatsReader statsReader, KylinConfig kylinConfig) {
        double totalSize = 0;
        for (double x: statsReader.getCuboidSizeMap().values()) {
            totalSize += x;
        }
        float rddCut = kylinConfig.getSparkRDDPartitionCutMB();
        int partition = (int) (totalSize / rddCut);
        partition = Math.max(kylinConfig.getSparkMinPartition(), partition);
        partition = Math.min(kylinConfig.getSparkMaxPartition(), partition);
        return partition;
    }

}
