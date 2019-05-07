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

package org.apache.kylin.common;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.stream.Collectors;

public class MissingMeasureSegment {

    public static String getKey(String projectName, String cubeName, String segmentName) {
        return projectName + "/" + cubeName + "/" + segmentName;
    }

    private String projectName;

    private String cubeName;

    private String segmentName;

    private Set<String> missMeasures;

    public MissingMeasureSegment(String projectName, String cubeName, String segmentName) {
        this.projectName = projectName;
        this.cubeName = cubeName;
        this.segmentName = segmentName;
    }

    public String getKey() {
        return getKey(projectName, cubeName, segmentName);
    }

    public String getMissingMsg(){
        return cubeName + "-" + segmentName + " missing measures: " + missMeasures.stream().collect(Collectors.joining(", "));
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public Set<String> getMissMeasures() {
        if (missMeasures == null) {
            missMeasures = Sets.newHashSet();
        }
        return missMeasures;
    }

    public void setMissMeasures(Set<String> missMeasures) {
        this.missMeasures = missMeasures;
    }
}
