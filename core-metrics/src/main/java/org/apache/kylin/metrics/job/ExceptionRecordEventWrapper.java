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

package org.apache.kylin.metrics.job;

import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.RecordEventWrapper;

public class ExceptionRecordEventWrapper extends RecordEventWrapper {

    public ExceptionRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
    }

    public <T extends Throwable> void setWrapper(String projectName, String cubeName, String jobId, String jobType,
            String cubingType, Class<T> exceptionClassName) {
        this.metricsEvent.put(JobPropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(JobPropertyEnum.CUBE.toString(), cubeName);
        this.metricsEvent.put(JobPropertyEnum.ID_CODE.toString(), jobId);
        this.metricsEvent.put(JobPropertyEnum.TYPE.toString(), jobType);
        this.metricsEvent.put(JobPropertyEnum.ALGORITHM.toString(), cubingType);
        this.metricsEvent.put(JobPropertyEnum.EXCEPTION.toString(), exceptionClassName.getName());
    }

}