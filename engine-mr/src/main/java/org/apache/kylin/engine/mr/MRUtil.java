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

package org.apache.kylin.engine.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.lookup.LookupProviderFactory;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchMergeInputSide;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchMergeOutputSide2;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.StorageFactory;

public class MRUtil {

    public static IMRBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        return (IMRBatchCubingInputSide)SourceManager.createEngineAdapter(seg, IMRInput.class).getBatchCubingInputSide(flatDesc);
    }

    public static IMRTableInputFormat getTableInputFormat(String tableName, String prj, String uuid) {
        TableDesc t = getTableDesc(tableName, prj);
        return SourceManager.createEngineAdapter(t, IMRInput.class).getTableInputFormat(t, uuid);
    }

    public static IMRTableInputFormat getTableInputFormat(TableDesc tableDesc, String uuid) {
        return SourceManager.createEngineAdapter(tableDesc, IMRInput.class).getTableInputFormat(tableDesc, uuid);
    }

    private static TableDesc getTableDesc(String tableName, String prj) {
        return TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(tableName, prj);
    }

    public static IMRBatchCubingOutputSide2 getBatchCubingOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchCubingOutputSide(seg);
    }

    public static IMRBatchMergeOutputSide2 getBatchMergeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchMergeOutputSide(seg);
    }

    public static IMRBatchMergeInputSide getBatchMergeInputSide(CubeSegment seg) {
        return (IMRBatchMergeInputSide)SourceManager.createEngineAdapter(seg, IMRInput.class).getBatchMergeInputSide(seg);
    }

    public static IMROutput2.IMRBatchOptimizeOutputSide2 getBatchOptimizeOutputSide2(CubeSegment seg) {
        return StorageFactory.createEngineAdapter(seg, IMROutput2.class).getBatchOptimizeOutputSide(seg);
    }

    public static ILookupMaterializer getExtLookupMaterializer(String lookupStorageType) {
        return LookupProviderFactory.createEngineAdapter(lookupStorageType, ILookupMaterializer.class);
    }
    
    // use this method instead of ToolRunner.run() because ToolRunner.run() is not thread-sale
    // Refer to: http://stackoverflow.com/questions/22462665/is-hadoops-toorunner-thread-safe
    public static int runMRJob(Tool tool, String[] args) throws Exception {
        Configuration conf = tool.getConf();
        if (conf == null) {
            conf = new Configuration();
        }

        GenericOptionsParser parser = getParser(conf, args);
        //set the configuration back, so that Tool can configure itself
        tool.setConf(conf);

        //get the args w/o generic hadoop args
        String[] toolArgs = parser.getRemainingArgs();
        return tool.run(toolArgs);
    }

    private static synchronized GenericOptionsParser getParser(Configuration conf, String[] args) throws Exception {
        return new GenericOptionsParser(conf, args);
    }
}
