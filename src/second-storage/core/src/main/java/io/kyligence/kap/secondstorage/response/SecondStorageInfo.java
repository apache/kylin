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

package io.kyligence.kap.secondstorage.response;


import java.util.List;
import java.util.Map;

public class SecondStorageInfo {
    private long secondStorageSize;
    private Map<String, List<SecondStorageNode>> secondStorageNodes;
    private boolean secondStorageEnabled;

    public long getSecondStorageSize() {
        return secondStorageSize;
    }

    public SecondStorageInfo setSecondStorageSize(long secondStorageSize) {
        this.secondStorageSize = secondStorageSize;
        return this;
    }

    public Map<String, List<SecondStorageNode>> getSecondStorageNodes() {
        return secondStorageNodes;
    }

    public SecondStorageInfo setSecondStorageNodes(Map<String, List<SecondStorageNode>> secondStorageNodes) {
        this.secondStorageNodes = secondStorageNodes;
        return this;
    }

    public boolean isSecondStorageEnabled() {
        return secondStorageEnabled;
    }

    public SecondStorageInfo setSecondStorageEnabled(boolean secondStorageEnabled) {
        this.secondStorageEnabled = secondStorageEnabled;
        return this;
    }
}
