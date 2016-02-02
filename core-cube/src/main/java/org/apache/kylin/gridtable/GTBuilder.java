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

public class GTBuilder implements Closeable {

    @SuppressWarnings("unused")
    final private GTInfo info;
    final private IGTWriter storeWriter;

    private int writtenRowCount;

    GTBuilder(GTInfo info, int shard, IGTStore store) throws IOException {
        this(info, shard, store, false);
    }

    GTBuilder(GTInfo info, int shard, IGTStore store, boolean append) throws IOException {
        this.info = info;

        if (append) {
            storeWriter = store.append();
        } else {
            storeWriter = store.rebuild();
        }
    }

    public void write(GTRecord r) throws IOException {
        storeWriter.write(r);
        writtenRowCount++;
    }

    @Override
    public void close() throws IOException {
        storeWriter.close();
    }

    public int getWrittenRowCount() {
        return writtenRowCount;
    }

}
