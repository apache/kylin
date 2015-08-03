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

package org.apache.kylin.job.cube;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kylin.job.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.Output;

/**
 * Created by qianzhou on 12/25/14.
 */
public class CubingJob extends DefaultChainedExecutable {

    public CubingJob() {
        super();
    }

    private static final String CUBE_INSTANCE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    public static final String MAP_REDUCE_WAIT_TIME = "mapReduceWaitTime";

    public void setCubeName(String name) {
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
    protected Pair<String, String> formatNotifications(ExecutableState state) {
        final Output output = jobService.getOutput(getId());
        String logMsg;
        switch (output.getState()) {
        case ERROR:
            logMsg = output.getVerboseMsg();
            break;
        case DISCARDED:
            logMsg = "";
            break;
        case SUCCEED:
            logMsg = "";
            break;
        default:
            return null;
        }
        String content = ExecutableConstants.NOTIFY_EMAIL_TEMPLATE;
        content = content.replaceAll("\\$\\{job_name\\}", getName());
        content = content.replaceAll("\\$\\{result\\}", state.toString());
        content = content.replaceAll("\\$\\{cube_name\\}", getCubeName());
        content = content.replaceAll("\\$\\{start_time\\}", new Date(getStartTime()).toString());
        content = content.replaceAll("\\$\\{duration\\}", getDuration() / 60000 + "mins");
        content = content.replaceAll("\\$\\{mr_waiting\\}", getMapReduceWaitTime() / 60000 + "mins");
        content = content.replaceAll("\\$\\{last_update_time\\}", new Date(getLastModified()).toString());
        content = content.replaceAll("\\$\\{submitter\\}", getSubmitter());
        content = content.replaceAll("\\$\\{error_log\\}", logMsg);

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

}
