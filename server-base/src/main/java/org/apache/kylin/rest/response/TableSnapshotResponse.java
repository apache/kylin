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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;

public class TableSnapshotResponse implements Serializable {
    private static final long serialVersionUID = -8707176301793624704L;
    public static final String TYPE_EXT = "ext";
    public static final String TYPE_INNER = "inner";

    private String snapshotID;

    private String snapshotType; // can be ext or inner

    private String storageType;

    private long lastBuildTime;

    private long sourceTableSize;

    private long sourceTableLastModifyTime;

    private List<String> cubesAndSegmentsUsage;

    public String getSnapshotID() {
        return snapshotID;
    }

    public void setSnapshotID(String snapshotID) {
        this.snapshotID = snapshotID;
    }

    public String getSnapshotType() {
        return snapshotType;
    }

    public void setSnapshotType(String snapshotType) {
        this.snapshotType = snapshotType;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public long getSourceTableSize() {
        return sourceTableSize;
    }

    public void setSourceTableSize(long sourceTableSize) {
        this.sourceTableSize = sourceTableSize;
    }

    public long getSourceTableLastModifyTime() {
        return sourceTableLastModifyTime;
    }

    public void setSourceTableLastModifyTime(long sourceTableLastModifyTime) {
        this.sourceTableLastModifyTime = sourceTableLastModifyTime;
    }

    public List<String> getCubesAndSegmentsUsage() {
        return cubesAndSegmentsUsage;
    }

    public void setCubesAndSegmentsUsage(List<String> cubesAndSegmentsUsage) {
        this.cubesAndSegmentsUsage = cubesAndSegmentsUsage;
    }
}
