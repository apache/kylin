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

package org.apache.kylin.gridtable;

import java.io.Closeable;
import java.io.IOException;

public class GridTable implements Closeable {

    final GTInfo info;
    final IGTStore store;

    public GridTable(GTInfo info, IGTStore store) {
        this.info = info;
        this.store = store;
    }

    public GTBuilder rebuild() throws IOException {
        return rebuild(-1);
    }

    public GTBuilder rebuild(int shard) throws IOException {
        return new GTBuilder(info, shard, store);
    }

    public GTBuilder append() throws IOException {
        return append(-1);
    }

    public GTBuilder append(int shard) throws IOException {
        return new GTBuilder(info, shard, store, true);
    }

    public IGTScanner scan(GTScanRequest req) throws IOException {
        IGTScanner result = store.scan(req);
        return req.decorateScanner(result);
    }

    public GTInfo getInfo() {
        return info;
    }

    public IGTStore getStore() {
        return store;
    }

    @Override
    public void close() throws IOException {
        if (store instanceof Closeable) {
            ((Closeable) store).close();
        }
    }
}
