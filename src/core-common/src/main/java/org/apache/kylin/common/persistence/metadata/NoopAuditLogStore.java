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
package org.apache.kylin.common.persistence.metadata;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.UnitMessages;

import com.google.common.collect.Lists;

public class NoopAuditLogStore implements AuditLogStore {
    @Override
    public void save(UnitMessages unitMessages) {
        // just implement it
    }

    @Override
    public List<AuditLog> fetch(long currentId, long size) {
        return Lists.newArrayList();
    }

    @Override
    public long getMaxId() {
        return 0;
    }

    @Override
    public long getMinId() {
        return 0;
    }

    @Override
    public long getLogOffset() {
        return 0;
    }

    @Override
    public void restore(long currentId) {
        // just implement it
    }

    @Override
    public void rotate() {
        // just implement it
    }

    @Override
    public void catchupWithTimeout() throws Exception {
        //do nothing
    }

    @Override
    public void catchup() {
        //do nothing
    }

    @Override
    public void setInstance(String instance) {
        //do nothing
    }

    @Override
    public AuditLog get(String resPath, long mvcc) {
        return null;
    }

    @Override
    public void pause() {
        //do noting
    }

    @Override
    public void reInit() {
        //do nothing
    }

    @Override
    public void close() throws IOException {
        // just implement it
    }

}
