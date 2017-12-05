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

package org.apache.kylin.storage.druid.common;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ForeverLoadRule extends Rule {
    private final Map<String, Integer> tieredReplicants;

    @JsonCreator
    public ForeverLoadRule(@JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants) {
        this.tieredReplicants = tieredReplicants;
        validateTieredReplicants(tieredReplicants);
    }

    @JsonProperty
    @Override
    public String getType() {
        return "loadForever";
    }

    @JsonProperty
    public Map<String, Integer> getTieredReplicants() {
        return tieredReplicants;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ForeverLoadRule that = (ForeverLoadRule) o;
        return Objects.equals(tieredReplicants, that.tieredReplicants);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tieredReplicants);
    }
}
