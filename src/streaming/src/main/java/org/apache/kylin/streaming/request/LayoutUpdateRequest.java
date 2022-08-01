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
package org.apache.kylin.streaming.request;

import java.util.List;

import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;

import lombok.Data;

@Data
public class LayoutUpdateRequest extends StreamingRequestHeader {
    private String project;

    private String dataflowId;

    private List<NDataLayout> layouts;

    private List<NDataSegDetails> segDetails;

    public LayoutUpdateRequest() {

    }

    public LayoutUpdateRequest(String project, String dataflowId, List<NDataLayout> layouts,
            List<NDataSegDetails> segDetails) {
        this.project = project;
        this.dataflowId = dataflowId;
        this.layouts = layouts;
        this.segDetails = segDetails;
    }
}
