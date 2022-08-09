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

package org.apache.kylin.job.dao;

import static org.apache.kylin.job.constant.ExecutableConstants.YARN_APP_IDS_DELIMITER;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;

/**
 */
@Setter
@Getter
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExecutablePO extends RootPersistentEntity {
    public static final int HIGHEST_PRIORITY = 0;
    public static final int DEFAULT_PRIORITY = 3;
    public static final int LOWEST_PRIORITY = 4;
    @JsonProperty("name")
    private String name;

    @JsonProperty("tasks")
    private List<ExecutablePO> tasks;

    @JsonProperty("type")
    private String type;

    @JsonProperty("handler_type")
    private String handlerType;

    @JsonProperty("params")
    private Map<String, String> params = Maps.newHashMap();

    @JsonProperty("segments")
    private Set<NDataSegment> segments = Sets.newHashSet();

    @JsonProperty("job_type")
    private JobTypeEnum jobType;

    @JsonProperty("data_range_start")
    private long dataRangeStart;

    @JsonProperty("data_range_end")
    private long dataRangeEnd;

    @JsonProperty("target_model")
    private String targetModel;

    @JsonProperty("target_segments")
    private List<String> targetSegments;

    @JsonProperty("output")
    private ExecutableOutputPO output = new ExecutableOutputPO();

    private String project;

    @JsonProperty("target_partitions")
    private Set<Long> targetPartitions = Sets.newHashSet();

    @JsonProperty("priority")
    private int priority = DEFAULT_PRIORITY;

    @JsonProperty("tag")
    private Object tag;

    @JsonProperty("stages_map")
    private Map<String, List<ExecutablePO>> stagesMap;

    public void setPriority(int p) {
        priority = isPriorityValid(p) ? p : DEFAULT_PRIORITY;
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return new StringBuilder().append("/").append(project).append(ResourceStore.EXECUTABLE_JOB).append("/")
                .append(name).toString();
    }

    public static boolean isPriorityValid(int priority) {
        return priority >= HIGHEST_PRIORITY && priority <= LOWEST_PRIORITY;
    }

    public static boolean isHigherPriority(int p1, int p2) {
        return p1 < p2;
    }

    public void addYarnApplicationJob(String appId) {
        String oldAppIds = output.getInfo().getOrDefault(ExecutableConstants.YARN_APP_IDS, "");
        Set<String> appIds = new HashSet<>(Arrays.asList(oldAppIds.split(YARN_APP_IDS_DELIMITER)));
        if (!appIds.contains(appId)) {
            String newAppIds = oldAppIds + (StringUtils.isEmpty(oldAppIds) ? "" : YARN_APP_IDS_DELIMITER) + appId;
            output.getInfo().put(ExecutableConstants.YARN_APP_IDS, newAppIds);
        }
    }
}
