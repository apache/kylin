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
package org.apache.kylin.common.persistence;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.event.StartUnit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UnitMessages {

    private List<Event> messages = Lists.newArrayList();

    @JsonIgnore
    public String getKey() {
        if (isEmpty()) {
            return null;
        }
        return messages.get(0).getKey();
    }

    @JsonIgnore
    public String getUnitId() {
        if (isEmpty() || !(messages.get(0) instanceof StartUnit)) {
            return null;
        }
        return ((StartUnit) messages.get(0)).getUnitId();
    }

    @JsonIgnore
    public String getInstance() {
        if (isEmpty()) {
            return null;
        }
        return messages.get(0).getInstance();
    }

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(messages);
    }
}
