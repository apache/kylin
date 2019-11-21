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

package org.apache.kylin.stream.coordinator.exception;

import org.apache.kylin.stream.core.exception.StreamingException;

import java.util.Locale;

public class ClusterStateException extends StreamingException {

    @SuppressWarnings("unused")
    private final String cubeName;
    private final ClusterState clusterState;
    private final TransactionStep transactionStep;
    private final String inconsistentPart;

    public ClusterState getClusterState() {
        return clusterState;
    }

    public TransactionStep getTransactionStep() {
        return transactionStep;
    }

    @SuppressWarnings("unused")
    public String getInconsistentPart() {
        return inconsistentPart;
    }

    public ClusterStateException(String cubeName, ClusterState state, TransactionStep step, String failedRs,
            Throwable cause) {
        super(String.format(Locale.ROOT, "Cube: %s    State: %s    Step: %s    Affect: %s", cubeName, state.name(),
                step.name(), failedRs), cause);
        this.cubeName = cubeName;
        this.transactionStep = step;
        this.clusterState = state;
        this.inconsistentPart = failedRs;
    }

    public enum ClusterState {
        CONSISTENT, ROLLBACK_SUCCESS, ROLLBACK_FAILED
    }

    public enum TransactionStep {
        STOP_AND_SNYC, ASSIGN_NEW, START_NEW, MAKE_IMMUTABLE
    }
}
