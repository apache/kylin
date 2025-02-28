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

import java.util.concurrent.atomic.AtomicLong;

import lombok.Getter;

/**
 */
public class VersionedRawResource {

    @Getter
    private RawResource rawResource;

    //CANNOT expose this!
    //In theory, mvcc is not necessary since we have project lock for all meta change,update,delete actions
    //we keep it just in case project lock protocol is breached somewhere
    private AtomicLong mvcc;

    public VersionedRawResource(RawResource rawResource) {
        this.mvcc = new AtomicLong(rawResource.getMvcc());
        this.rawResource = rawResource;
    }

    public void update(RawResource r) {
        if (mvcc.compareAndSet(r.getMvcc() - 1, r.getMvcc())) {
            this.rawResource = r;
        } else {
            throw new VersionConflictException(rawResource, r, "Overwriting conflict " + r.getMetaKey()
                    + ", expect old mvcc: " + (r.getMvcc() - 1) + ", but found: " + mvcc.get());
        }
    }

    public long getMvcc() {
        return mvcc.get();
    }
}
