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

package org.apache.kylin.common.persistence.event;

import java.io.Serializable;

import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.AuditLog;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import lombok.Data;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public class Event implements Serializable {

    private String key;

    private String instance;

    public static Event fromLog(AuditLog log) {
        Event event;
        if (log.getByteSource() == null) {
            event = new ResourceDeleteEvent(log.getResPath());
        } else {
            event = new ResourceCreateOrUpdateEvent(
                    new RawResource(log.getResPath(), log.getByteSource(), log.getTimestamp(), log.getMvcc()));
        }
        if (log.getResPath().startsWith(ResourceStore.PROJECT_ROOT)) {
            event.setKey(log.getResPath().substring(ResourceStore.PROJECT_ROOT.length() + 1).replace(".json", ""));
        } else {
            event.setKey(log.getResPath().split("/")[1]);
        }
        event.setInstance(log.getInstance());
        return event;
    }

}
