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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;

/**
 */
public class CubingJob extends DefaultChainedExecutable {
    
    public static enum AlgorithmEnum {
        LAYER, INMEM
    }

    // KEYS of Output.extraInfo map, info passed across job steps
    public static final String SOURCE_RECORD_COUNT = "sourceRecordCount";
    public static final String SOURCE_SIZE_BYTES = "sourceSizeBytes";
    public static final String CUBE_SIZE_BYTES = "byteSizeBytes";
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";

    private static final String CUBE_INSTANCE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";

    public static CubingJob createBuildJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, "BUILD", submitter, config);
    }

    public static CubingJob createMergeJob(CubeSegment seg, String submitter, JobEngineConfig config) {
        return initCubingJob(seg, "MERGE", submitter, config);
    }

    private static CubingJob initCubingJob(CubeSegment seg, String jobType, String submitter, JobEngineConfig config) {
        CubingJob result = new CubingJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setCubeName(seg.getCubeInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setName(seg.getCubeInstance().getName() + " - " + seg.getName() + " - " + jobType + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        result.setNotifyList(seg.getCubeInstance().getDescriptor().getNotifyList());
        return result;
    }

    public CubingJob() {
        super();
    }

    void setCubeName(String name) {
        setParam(CUBE_INSTANCE_NAME, name);
    }

    public String getCubeName() {
        return getParam(CUBE_INSTANCE_NAME);
    }

    void setSegmentIds(List<String> segmentIds) {
        setParam(SEGMENT_ID, StringUtils.join(segmentIds, ","));
    }

    void setSegmentId(String segmentId) {
        setParam(SEGMENT_ID, segmentId);
    }

    public String getSegmentIds() {
        return getParam(SEGMENT_ID);
    }

    @Override
    protected Pair<String, String> formatNotifications(ExecutableContext context, ExecutableState state) {
        CubeInstance cubeInstance = CubeManager.getInstance(context.getConfig()).getCube(getCubeName());
        final Output output = jobService.getOutput(getId());
        String logMsg;
        state = output.getState();
        if (state != ExecutableState.ERROR && !cubeInstance.getDescriptor().getStatusNeedNotify().contains(state.toString().toLowerCase())) {
            logger.info("state:" + state + " no need to notify users");
            return null;
        }
        switch (state) {
        case ERROR:
            logMsg = output.getVerboseMsg();
            break;
        case DISCARDED:
            logMsg = "job has been discarded";
            break;
        case SUCCEED:
            logMsg = "job has succeeded";
            break;
        default:
            return null;
        }
        String content = ExecutableConstants.NOTIFY_EMAIL_TEMPLATE;
        content = content.replaceAll("\\$\\{job_name\\}", getName());
        content = content.replaceAll("\\$\\{result\\}", state.toString());
        content = content.replaceAll("\\$\\{cube_name\\}", getCubeName());
        content = content.replaceAll("\\$\\{source_records_count\\}", String.valueOf(findSourceRecordCount()));
        content = content.replaceAll("\\$\\{start_time\\}", new Date(getStartTime()).toString());
        content = content.replaceAll("\\$\\{duration\\}", getDuration() / 60000 + "mins");
        content = content.replaceAll("\\$\\{mr_waiting\\}", getMapReduceWaitTime() / 60000 + "mins");
        content = content.replaceAll("\\$\\{last_update_time\\}", new Date(getLastModified()).toString());
        content = content.replaceAll("\\$\\{submitter\\}", StringUtil.noBlank(getSubmitter(), "missing submitter"));
        content = content.replaceAll("\\$\\{error_log\\}", Matcher.quoteReplacement(StringUtil.noBlank(logMsg, "no error message")));

        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            content = content.replaceAll("\\$\\{job_engine\\}", inetAddress.getCanonicalHostName());
        } catch (UnknownHostException e) {
            logger.warn(e.getLocalizedMessage(), e);
        }

        String title = "[" + state.toString() + "] - [Kylin Cube Build Job]-" + getCubeName();
        return Pair.of(title, content);
    }

    @Override
    protected void onExecuteFinished(ExecuteResult result, ExecutableContext executableContext) {
        long time = 0L;
        for (AbstractExecutable task : getTasks()) {
            final ExecutableState status = task.getStatus();
            if (status != ExecutableState.SUCCEED) {
                break;
            }
            if (task instanceof MapReduceExecutable) {
                time += ((MapReduceExecutable) task).getMapReduceWaitTime();
            }
        }
        setMapReduceWaitTime(time);
        super.onExecuteFinished(result, executableContext);
    }

    public long getMapReduceWaitTime() {
        return getExtraInfoAsLong(MAP_REDUCE_WAIT_TIME, 0L);
    }

    public void setMapReduceWaitTime(long t) {
        addExtraInfo(MAP_REDUCE_WAIT_TIME, t + "");
    }
    
    public void setAlgorithm(AlgorithmEnum alg) {
        addExtraInfo("algorithm", alg.name());
    }
    
    public AlgorithmEnum getAlgorithm() {
        String alg = getExtraInfo().get("algorithm");
        return alg == null ? null : AlgorithmEnum.valueOf(alg);
    }

    public boolean isLayerCubing() {
        return AlgorithmEnum.LAYER == getAlgorithm();
    }
    
    public boolean isInMemCubing() {
        return AlgorithmEnum.INMEM == getAlgorithm();
    }
    
    public long findSourceRecordCount() {
        return Long.parseLong(findExtraInfo(SOURCE_RECORD_COUNT, "0"));
    }

    public long findSourceSizeBytes() {
        return Long.parseLong(findExtraInfo(SOURCE_SIZE_BYTES, "0"));
    }

    public long findCubeSizeBytes() {
        // look for the info BACKWARD, let the last step that claims the cube size win
        return Long.parseLong(findExtraInfoBackward(CUBE_SIZE_BYTES, "0"));
    }
    
    public String findExtraInfo(String key, String dft) {
        return findExtraInfo(key, dft, false);
    }

    public String findExtraInfoBackward(String key, String dft) {
        return findExtraInfo(key, dft, true);
    }
    
    private String findExtraInfo(String key, String dft, boolean backward) {
        ArrayList<AbstractExecutable> tasks = new ArrayList<AbstractExecutable>(getTasks());
        
        if (backward) {
            Collections.reverse(tasks);
        }
        
        for (AbstractExecutable child : tasks) {
            Output output = executableManager.getOutput(child.getId());
            String value = output.getExtra().get(key);
            if (value != null)
                return value;
        }
        return dft;
    }

}
