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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Multimap;
import org.apache.kylin.guava30.shaded.common.collect.Multimaps;
import org.apache.kylin.job.constant.JobStatusEnum;

/**
 */
public enum ExecutableState {

    READY, PENDING, RUNNING, ERROR, PAUSED, DISCARDED, SUCCEED, SUICIDAL, SKIP, WARNING;

    private static Multimap<ExecutableState, ExecutableState> VALID_STATE_TRANSFER;

    static {
        VALID_STATE_TRANSFER = Multimaps.newSetMultimap(Maps.newEnumMap(ExecutableState.class),
                () -> new CopyOnWriteArraySet<>());

        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.PENDING);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.PAUSED);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.READY, ExecutableState.SUICIDAL);

        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.READY);
        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.RUNNING);
        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.ERROR);
        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.PENDING, ExecutableState.PAUSED);

        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.READY);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SUCCEED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.ERROR);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.PAUSED);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.SKIP);
        VALID_STATE_TRANSFER.put(ExecutableState.RUNNING, ExecutableState.WARNING);

        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.PAUSED, ExecutableState.READY);

        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.SUICIDAL);
        VALID_STATE_TRANSFER.put(ExecutableState.ERROR, ExecutableState.READY);

        VALID_STATE_TRANSFER.put(ExecutableState.SUCCEED, ExecutableState.READY);

    }

    public static ExecutableState[] getFinalStates() {
        return new ExecutableState[] { SUCCEED, DISCARDED, SUICIDAL };
    }

    public static List<ExecutableState> getNotFinalStates() {
        return Arrays.stream(values()).filter(o -> !o.isFinalState()).collect(Collectors.toList());
    }

    public static List<String> getNotFinalStateNames() {
        return getNotFinalStates().stream().map(Enum::name).collect(Collectors.toList());
    }

    public boolean isProgressing() {
        return this == READY || this == RUNNING || this == PENDING;
    }

    public boolean isFinalState() {
        return Lists.newArrayList(getFinalStates()).contains(this);
    }

    public boolean isRunning() {
        return !isFinalState();
    }

    public boolean isNotProgressing() {
        return this == ERROR || this == PAUSED;
    }

    public boolean isStoppedNonVoluntarily() {
        return this == DISCARDED || this == PAUSED //
                || this == READY || this == PENDING;//restart case
    }

    public boolean isNotBad() {
        return this == SUCCEED || this == SKIP || this == WARNING;
    }

    public static boolean isValidStateTransfer(ExecutableState from, ExecutableState to) {
        return VALID_STATE_TRANSFER.containsEntry(from, to);
    }

    public JobStatusEnum toJobStatus() {
        switch (this) {
        case SKIP:
            return JobStatusEnum.SKIP;
        case READY:
        case PENDING:
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
        case WARNING:
            return JobStatusEnum.WARNING;
        default:
            throw new RuntimeException("invalid state:" + this);
        }
    }

    public String toStringState() {
        switch (this) {
            case SUCCEED:
                return "Succeed";
            case ERROR:
                return "Error";
            case DISCARDED:
                return "Discard";
            default:
                throw new IllegalStateException("invalid Executable state:" + this);
        }
    }
}
