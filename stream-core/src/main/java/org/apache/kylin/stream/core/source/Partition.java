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

package org.apache.kylin.stream.core.source;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Kylin streaming partition, represents how the streaming cube is partitioned
 * across the replicaSets.
 *
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Partition implements Comparable<Partition> {
    @JsonProperty("partition_id")
    private int partitionId;
    @JsonProperty("partition_info")
    private String partitionInfo;

    public Partition(int partitionId) {
        this.partitionId = partitionId;
    }

    public Partition() {
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public String getPartitionInfo() {
        return partitionInfo;
    }

    public void setPartitionInfo(String partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + partitionId;
        result = prime * result + ((partitionInfo == null) ? 0 : partitionInfo.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Partition other = (Partition) obj;
        if (partitionId != other.partitionId)
            return false;
        if (partitionInfo == null) {
            if (other.partitionInfo != null)
                return false;
        } else if (!partitionInfo.equals(other.partitionInfo))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Partition [partitionId=" + partitionId + ", partitionInfo=" + partitionInfo + "]";
    }

    @Override
    public int compareTo(Partition other) {
        return getPartitionId() - other.getPartitionId();
    }
}