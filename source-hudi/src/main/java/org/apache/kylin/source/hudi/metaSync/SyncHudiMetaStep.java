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
package org.apache.kylin.source.hudi.metaSync;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public class SyncHudiMetaStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(SyncHudiMetaStep.class);
    private final PatternedLogger stepLogger = new PatternedLogger(logger);
    private DataModelDesc dataModelDesc = null;
    
    public void setCmd(String cmd){ setParam("cmd",cmd);}
    
    public SyncHudiMetaStep(DataModelDesc dataModelDesc) throws IOException {
        this.dataModelDesc = dataModelDesc;
    }
    
    public void syncHudiTables() throws IOException {
        String cmd = getParam("cmd");
        JoinTableDesc[] joinTableDescs = this.dataModelDesc.getJoinTables();
        for(JoinTableDesc joinTable:joinTableDescs){
            cmd = String.format(Locale.ROOT,"%s/run_sync_tool.sh "
             + generateHudiConfigArgString(joinTable) + cmd, this.getConfig());
            stepLogger.log(String.format(Locale.ROOT,"exe cmd:%s",cmd));
            Pair<Integer,String> response = this.getConfig().getCliCommandExecutor().execute(cmd,stepLogger);
            getManager().addJobInfo(getId(),stepLogger.getInfo());
            if(response.getFirst()!=0){
                throw new RuntimeException("Failed to Synchronize the Hudi metadata into hive, error code "+response.getFirst());
            }
        }
    }

    private String generateHudiConfigArgString(JoinTableDesc joinTable) {
        KylinConfig kylinConfig = getConfig();
        Map<String,String> config = Maps.newHashMap();
        config.putAll(kylinConfig.parseHudiConfigParams());

        StringBuilder args = new StringBuilder();
        for(Map.Entry<String,String> entry:config.entrySet()){
            args.append(" --"+ entry.getKey()+" "+ entry.getValue()+"\r\n");
        }
        args.append(" --table "+joinTable.getTable()+"\r\n");
        return args.toString();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException, PersistentException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try{
            syncHudiTables();
            return new ExecuteResult(ExecuteResult.State.SUCCEED,stepLogger.getBufferedLog());

        }catch (Exception e){
            logger.error("job:"+getId()+" execute finished with exception",e);
            return new ExecuteResult(ExecuteResult.State.FAILED,stepLogger.getBufferedLog(),e);
        }
    }


}
