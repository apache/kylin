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

package org.apache.kylin.job.execution;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.kylin.job.constant.JobStatusEnum;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 */
public enum ExecutableState {

    READY, RUNNING, ERROR, PAUSED, DISCARDED, SUCCEED, SUICIDAL, SKIP;

    private static Multimap<ExecutableState, ExecutableState> VALID_STATE_TRANSFER;

    static {
        VALID_STATE_TRANSFER = Multimaps.newSetMultimap(
                Maps.<ExecutableState, Collection<ExecutableState>> newEnumMap(ExecutableState.class),
                new Supplier<Set<ExecutableState>>() {
                    @Override
                    public Set<ExecutableState> get() {
                        return new CopyOnWriteArraySet<ExecutableState>();
                    }
                });

        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.RUNNING);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.ERROR);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.PAUSED);

        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.READY);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SUCCEED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.ERROR);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.PAUSED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SKIP);

        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.READY);

        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.READY);

        VALID_STATE_TRANSFER.put(ExecutableState.SUCCEED, ExecutableState.READY);

    }

    public boolean isProgressing() {
        return this == READY || this == RUNNING;
    }

    public boolean isFinalState() {
        return this == SUCCEED || this == DISCARDED || this == SUICIDAL;
    }

    public boolean isRunning() {
        return !isFinalState();
    }

    public boolean isNotProgressing() {
        return this == ERROR || this == PAUSED;
    }

    public boolean isStoppedNonVoluntarily() {
        return this == DISCARDED || this == PAUSED || //
                this == READY;//restart case
    }

    public static boolean isValidStateTransfer(ExecutableState from, ExecutableState to) {
        return VALID_STATE_TRANSFER.containsEntry(from, to);
    }

    public JobStatusEnum toJobStatus() {
        switch (this) {
        case SKIP:
            return JobStatusEnum.SKIP;
        case READY:
            return JobStatusEnum.PENDING;
        case RUNNING:
            return JobStatusEnum.RUNNING;
        case ERROR:
            return JobStatusEnum.ERROR;
        case SUCCEED:
            return JobStatusEnum.FINISHED;
        case PAUSED:
            return JobStatusEnum.STOPPED;
        case SUICIDAL:
        case DISCARDED:
            return JobStatusEnum.DISCARDED;
        default:
            throw new RuntimeException("invalid state:" + this);
        }
    }
}
