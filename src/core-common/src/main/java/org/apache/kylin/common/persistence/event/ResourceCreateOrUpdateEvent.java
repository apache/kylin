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

import org.apache.kylin.common.persistence.RawResource;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class ResourceCreateOrUpdateEvent extends ResourceRelatedEvent {

    @Setter
    private String resPath;

    private RawResource createdOrUpdated;

    /**
     * Only when mvcc>0 (ie: update), will be used to apply content's diff
     * @return audit content
     */
    public byte[] getMetaContent() {
        return createdOrUpdated.getMvcc() > 0 && createdOrUpdated.getContentDiff() != null
                ? createdOrUpdated.getContentDiff()
                : createdOrUpdated.getContent();
    }
}
