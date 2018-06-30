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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.StorageFactory;

public class SparkUtil {

    public static ISparkInput.ISparkBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        return SourceManager.createEngineAdapter(seg, ISparkInput.class).getBatchCubingInputSide(flatDesc);
    }

    private static TableDesc getTableDesc(String tableName, String prj) {
        return TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(tableName, prj);
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

    private static synchronized GenericOptionsParser getParser(Configuration conf, String[] args) throws Exception {
        return new GenericOptionsParser(conf, args);
    }
}
