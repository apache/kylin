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

import lombok.Data;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

@Data
public class AlertMessageRequest implements Serializable {

    private String receiver;

    private String status;

    @NotEmpty
    private List<Alerts> alerts;

    private GroupLabels groupLabels;

    @Data
    public static class Alerts implements Serializable {
        private String status;
        @NotNull
        private Labels labels;
        private Annotations annotations;
    }

    @Data
    public static class GroupLabels implements Serializable {
        private String alertname;
    }

    @Data
    public static class Labels implements Serializable {
        private String alertname;
        private String instance;
        private String job;
        private String severity;
    }

    @Data
    public static class Annotations implements Serializable {
        private String description;
        private String summary;
    }
}
