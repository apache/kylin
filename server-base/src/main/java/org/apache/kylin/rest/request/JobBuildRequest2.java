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

package org.apache.kylin.rest.request;

import java.util.Map;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class JobBuildRequest2 {
    
    private long sourceOffsetStart;

    private long sourceOffsetEnd;

    private Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();

    private Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();

    private String buildType;

    private boolean force;
    
    private Integer priorityOffset = 0;

    public long getSourceOffsetStart() {
        return sourceOffsetStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    public long getSourceOffsetEnd() {
        return sourceOffsetEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    public Map<Integer, Long> getSourcePartitionOffsetStart() {
        return sourcePartitionOffsetStart;
    }

    public void setSourcePartitionOffsetStart(Map<Integer, Long> sourcePartitionOffsetStart) {
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
    }

    public Map<Integer, Long> getSourcePartitionOffsetEnd() {
        return sourcePartitionOffsetEnd;
    }

    public void setSourcePartitionOffsetEnd(Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    public String getBuildType() {
        return buildType;
    }

    public void setBuildType(String buildType) {
        this.buildType = buildType;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public Integer getPriorityOffset() {
        return priorityOffset;
    }

    public void setPriorityOffset(Integer priorityOffset) {
        this.priorityOffset = priorityOffset;
    }
}
