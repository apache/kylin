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

package org.apache.kylin.stream.source.kafka;

import java.util.List;

import org.apache.kafka.common.TopicPartition;

public class KafkaTopicAssignment {
    private Integer replicaSetID;
    private List<TopicPartition> assignments;

    public KafkaTopicAssignment(Integer replicaSetID, List<TopicPartition> assignments) {
        this.replicaSetID = replicaSetID;
        this.assignments = assignments;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((assignments == null) ? 0 : assignments.hashCode());
        result = prime * result + ((replicaSetID == null) ? 0 : replicaSetID.hashCode());
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
        KafkaTopicAssignment other = (KafkaTopicAssignment) obj;
        if (assignments == null) {
            if (other.assignments != null)
                return false;
        } else if (!assignments.equals(other.assignments))
            return false;
        if (replicaSetID == null) {
            if (other.replicaSetID != null)
                return false;
        } else if (!replicaSetID.equals(other.replicaSetID))
            return false;
        return true;
    }
}
