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
package org.apache.kylin.common.persistence.transaction;

import java.util.function.Consumer;

import org.apache.kylin.common.persistence.event.ResourceRelatedEvent;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UnitOfWorkParams<T> {

    private UnitOfWork.Callback<T> processor;

    private UnitOfWork.Callback<T> epochChecker;

    private Consumer<ResourceRelatedEvent> writeInterceptor;

    @Builder.Default
    private boolean all = false;

    @Builder.Default
    private String unitName = UnitOfWork.GLOBAL_UNIT;

    @Builder.Default
    private long epochId = UnitOfWork.DEFAULT_EPOCH_ID;

    @Builder.Default
    private int maxRetry = 3;

    @Builder.Default
    private boolean readonly = false;

    @Builder.Default
    private boolean useSandbox = true;

    @Builder.Default
    private boolean skipAuditLog = false;

    private String tempLockName;

}
