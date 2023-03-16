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

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.Getter;

/**
 */
public final class ExecuteResult {

    public enum State {
        SUCCEED, ERROR, SKIP
    }

    private final State state;
    private final String output;
    private final Throwable throwable;

    //extra
    @Getter
    private Map<String, String> extraInfo = Maps.newHashMap();

    private ExecuteResult(State state, String output, Throwable throwable) {
        Preconditions.checkArgument(state != null, "state cannot be null");

        switch (state) {
            case SUCCEED:
            case SKIP:
                Preconditions.checkNotNull(output);
                Preconditions.checkState(throwable == null);
                break;
            case ERROR:
                Preconditions.checkNotNull(throwable);
                Preconditions.checkState(output == null);
                break;
            default:
                throw new IllegalStateException();
        }

        this.state = state;
        this.output = output;
        this.throwable = throwable;
    }

    public static ExecuteResult createSucceed() {
        return new ExecuteResult(State.SUCCEED, "succeed", null);
    }

    public static ExecuteResult createSucceed(String output) {
        return new ExecuteResult(State.SUCCEED, output, null);
    }

    public static ExecuteResult createSkip() {
        return new ExecuteResult(State.SKIP, "skip", null);
    }

    public static ExecuteResult createSkip(String output) {
        return new ExecuteResult(State.SKIP, output, null);
    }

    public static ExecuteResult createError(Throwable throwable) {
        Preconditions.checkArgument(throwable != null, "throwable cannot be null");
        return new ExecuteResult(State.ERROR, null, throwable);
    }

    public State state() {
        return state;
    }

    public boolean succeed() {
        return state == State.SUCCEED;
    }

    public boolean skip() {
        return state == State.SKIP;
    }

    public String output() {
        return output;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public String getErrorMsg() {
        if (succeed()) {
            return null;
        }

        if (throwable != null) {
            return Throwables.getStackTraceAsString(throwable);
        } else if (StringUtils.isNotEmpty(output)) {
            return output;
        } else {
            return "error";
        }
    }

    public String getShortErrMsg() {
        if (succeed()) {
            return null;
        }
        if (throwable != null) {
            String msg = Throwables.getRootCause(throwable).getMessage();
            if (msg != null && msg.length() > 1000) {
                return msg.substring(0, 997) + "...";
            }
            return msg;
        } else {
            return null;
        }
    }
}
