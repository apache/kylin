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

import java.util.List;
import java.util.function.Consumer;

import org.apache.kylin.common.persistence.event.ResourceRelatedEvent;
import org.apache.kylin.common.persistence.lock.TransactionLock;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder
public class UnitOfWorkParams<T> {

    private UnitOfWork.Callback<T> processor;

    private UnitOfWork.Callback<T> epochChecker;

    private Consumer<ResourceRelatedEvent> writeInterceptor;

    private UnitRetryContext retryContext;

    private long retryUntil;

    @Builder.Default
    private boolean all = false;

    @Builder.Default
    private String unitName = UnitOfWork.GLOBAL_UNIT;

    private String projectId = "";

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

    @Builder.Default
    private boolean skipReplay = false;

    private String tempLockName;
    
    @Builder.Default
    private boolean useProjectLock = false;

    @Builder.Default
    private boolean retryMoreTimeForDeadLockException = false;

    /**
     * only for debug or test
     */
    @Builder.Default
    private long sleepMills = -1;

    @Getter
    @Setter
    @AllArgsConstructor
    public static class UnitRetryContext {
        private List<TransactionLock> retryLock;
        private boolean allowRetryNext;
        private boolean optimisticLockEnabled;
    }

}
