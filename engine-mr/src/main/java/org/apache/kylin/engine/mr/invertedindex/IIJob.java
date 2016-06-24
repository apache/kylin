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

package org.apache.kylin.engine.mr.invertedindex;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

/**
 */
public class IIJob extends DefaultChainedExecutable {

    public IIJob() {
        super();
    }

    private static final String II_INSTANCE_NAME = "iiName";
    private static final String SEGMENT_ID = "segmentId";

    void setIIName(String name) {
        setParam(II_INSTANCE_NAME, name);
    }

    public String getIIName() {
        return getParam(II_INSTANCE_NAME);
    }

    void setSegmentId(String segmentId) {
        setParam(SEGMENT_ID, segmentId);
    }

    public String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public static IIJob createBuildJob(IISegment seg, String submitter, JobEngineConfig config) {
        return initialJob(seg, "BUILD", submitter, config);
    }

    private static IIJob initialJob(IISegment seg, String type, String submitter, JobEngineConfig config) {
        IIJob result = new IIJob();
        SimpleDateFormat format = new SimpleDateFormat("z yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone(config.getTimeZone()));
        result.setIIName(seg.getIIInstance().getName());
        result.setSegmentId(seg.getUuid());
        result.setName(seg.getIIInstance().getName() + " - " + seg.getName() + " - " + type + " - " + format.format(new Date(System.currentTimeMillis())));
        result.setSubmitter(submitter);
        return result;
    }

}
