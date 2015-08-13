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

/**
 */
public class DefaultOutput implements Output {

    private ExecutableState state;
    private Map<String, String> extra;
    private String verboseMsg;
    private long lastModified;

    @Override
    public Map<String, String> getExtra() {
        return extra;
    }

    @Override
    public String getVerboseMsg() {
        return verboseMsg;
    }

    @Override
    public ExecutableState getState() {
        return state;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    public void setState(ExecutableState state) {
        this.state = state;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public void setVerboseMsg(String verboseMsg) {
        this.verboseMsg = verboseMsg;
    }

    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int hashCode = state.hashCode();
        hashCode = hashCode * prime + extra.hashCode();
        hashCode = hashCode * prime + verboseMsg.hashCode();
        hashCode = hashCode * prime + Long.valueOf(lastModified).hashCode();
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DefaultOutput)) {
            return false;
        }
        DefaultOutput another = ((DefaultOutput) obj);
        if (this.state != another.state) {
            return false;
        }
        if (!extra.equals(another.extra)) {
            return false;
        }
        if (this.lastModified != another.lastModified) {
            return false;
        }
        return StringUtils.equals(verboseMsg, another.verboseMsg);
    }
}
