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
package org.apache.kylin.source.hudi;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.source.hive.HiveInputBase;
import org.apache.kylin.source.hive.RedistributeFlatHiveTableByLivyStep;
import org.apache.kylin.source.hudi.metaSync.SyncHudiMetaStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HudiInputBase extends HiveInputBase {
    private static final Logger logger = LoggerFactory.getLogger(HudiInputBase.class);

    public static class HudiBaseBatchCubingInputSide extends BaseBatchCubingInputSide{
        public HudiBaseBatchCubingInputSide(IJoinedFlatTableDesc flatTableDesc){
            super(flatTableDesc);
        }

        protected KylinConfig getConfig() {return flatDesc.getDataModel().getConfig();}

        @Override
        protected void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow){

            final KylinConfig config = this.getConfig();
            if(config.isHudiMetaSync() == Boolean.FALSE){
                try{
                    jobFlow.addTask( new SyncHudiMetaStep(flatDesc.getDataModel()));
                }catch (IOException e){
                    logger.error("SyncHudiMeta Error",e);
                }
            }
            super.addStepPhase1_DoCreateFlatTable(jobFlow);
        }


    }


    protected static AbstractExecutable createRedistributeFlatHiveTableByLivyStep(String hiveInitStatements,
                                                                                  String cubeName, IJoinedFlatTableDesc flatDesc, CubeDesc cubeDesc) {
        cubeDesc.getConfig().setProperty("kylin.engine.spark-conf.spark.sql.hive.convertMetastoreParquet","false");
        return HiveInputBase.createRedistributeFlatHiveTableByLivyStep(hiveInitStatements,cubeName,flatDesc,cubeDesc);

    }


}
