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

import com.google.common.base.Preconditions;

/**
 */
public final class ExecuteResult {

    public static enum State {
        SUCCEED, FAILED, ERROR, DISCARDED, STOPPED
    }

    private final State state;
    private final String output;

    public ExecuteResult(State state) {
        this(state, "");
    }

    public ExecuteResult(State state, String output) {
        Preconditions.checkArgument(state != null, "state cannot be null");
        this.state = state;
        this.output = output;
    }

    public State state() {
        return state;
    }

    public boolean succeed() {
        return state == State.SUCCEED;
    }

    public String output() {
        return output;
    }
}
