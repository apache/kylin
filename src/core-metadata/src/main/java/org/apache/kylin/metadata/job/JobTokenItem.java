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

package org.apache.kylin.metadata.job;

import org.apache.kylin.common.annotation.Clarification;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Clarification(priority = Clarification.Priority.MAJOR)
public class JobTokenItem {

    private String jobId;
    private String token;
    private long createTime;
    private String reservedField1;
    private String reservedField2;

    public JobTokenItem() {
    }

    public JobTokenItem(String jobId, String token, long createTime) {
        this();
        this.jobId = jobId;
        this.token = token;
        this.createTime = createTime;
    }
}
