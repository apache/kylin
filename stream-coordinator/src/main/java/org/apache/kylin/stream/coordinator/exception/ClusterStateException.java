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
import java.util.Map;
import java.util.Set;

/**
 * This exception is to indicate that the receiver cluster is in
 * some inconsistent situation which maybe caused by failure of a distributed transaction.
 */
public class ClusterStateException extends StreamingException {

    private final String cubeName;
    private final ClusterState clusterState;

    /**
     *  The step of a distributed transaction which failed
     */
    private final TransactionStep transactionStep;

    /**
     *  The set of replica set which failed in roll back
     */
    private final Map<String, Set<Integer>> inconsistentRs;

    /**
     *  The id of replica set which cause the roll back
     */
    private final int failedRs;

    public ClusterState getClusterState() {
        return clusterState;
    }

    public TransactionStep getTransactionStep() {
        return transactionStep;
    }

    public Map<String, Set<Integer>> getInconsistentPart() {
        return inconsistentRs;
    }

    public ClusterStateException(String cubeName, String msg) {
        super(msg);
        this.cubeName = cubeName;
        this.clusterState = null;
        this.failedRs = -1;
        this.inconsistentRs = null;
        this.transactionStep = null;
    }

    public ClusterStateException(String cubeName, ClusterState state, TransactionStep step, int failedRs,
            Map<String, Set<Integer>> inconsistentRs, Throwable cause) {
        super(String.format(Locale.ROOT, "Cube:%s  State:%s  Step:%s  CausedBy:%s  Affect: %s", cubeName, state.name(),
                step.name(), failedRs, inconsistentRs), cause);
        this.cubeName = cubeName;
        this.transactionStep = step;
        this.clusterState = state;
        this.failedRs = failedRs;
        this.inconsistentRs = inconsistentRs;
    }

    public String getCubeName() {
        return cubeName;
    }

    public Map<String, Set<Integer>> getInconsistentRs() {
        return inconsistentRs;
    }

    public int getFailedRs() {
        return failedRs;
    }

    public enum ClusterState {
        CONSISTENT, ROLLBACK_SUCCESS, ROLLBACK_FAILED
    }

    public enum TransactionStep {
        STOP_AND_SNYC, ASSIGN_NEW, START_NEW, MAKE_IMMUTABLE
    }
}
