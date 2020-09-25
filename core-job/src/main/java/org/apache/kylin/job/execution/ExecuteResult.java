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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.util.Map;

/**
 */
public final class ExecuteResult {

    public static enum State {
        SUCCEED, FAILED, ERROR, DISCARDED, STOPPED
    }

    private final State state;
    private final String output;
    private final Throwable throwable;
    private Map<String, String> extraInfo = Maps.newHashMap();


    /**
     * Default constructor to indicate a success ExecuteResult.
     */
    public ExecuteResult() {
        this(State.SUCCEED, "succeed");
    }

    public ExecuteResult(State state) {
        this(state, "");
    }

    public ExecuteResult(State state, String output) {
        this(state, output, null);
    }

    public ExecuteResult(State state, String output, Throwable throwable) {
        Preconditions.checkArgument(state != null, "state cannot be null");
        this.state = state;
        this.output = output;
        this.throwable = throwable;
    }

    public static ExecuteResult createSucceed() {
        return new ExecuteResult(State.SUCCEED, "succeed");
    }

    public static ExecuteResult createSucceed(String output) {
        return new ExecuteResult(State.SUCCEED, output, null);
    }

    public static ExecuteResult createError(Throwable throwable) {
        Preconditions.checkArgument(throwable != null, "throwable cannot be null");
        return new ExecuteResult(State.ERROR, throwable.getLocalizedMessage(), throwable);
    }

    public static ExecuteResult createFailed(Throwable throwable) {
        Preconditions.checkArgument(throwable != null, "throwable cannot be null");
        return new ExecuteResult(State.FAILED, throwable.getLocalizedMessage(), throwable);
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

    public Throwable getThrowable() {
        return throwable;
    }

    public Map<String, String> getExtraInfo() {
        return extraInfo;
    }
}
