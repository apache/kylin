/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.kylin.cube.inmemcubing;

import org.apache.kylin.gridtable.GTRecord;

import java.io.IOException;

/**
 */
public class CompoundCuboidWriter implements ICuboidWriter {

    private Iterable<ICuboidWriter> cuboidWriters;

    public CompoundCuboidWriter(Iterable<ICuboidWriter> cuboidWriters) {
        this.cuboidWriters = cuboidWriters;

    }

    @Override
    public void write(long cuboidId, GTRecord record) throws IOException {
        for (ICuboidWriter writer : cuboidWriters) {
            writer.write(cuboidId, record);
        }
    }

    @Override
    public void flush() throws IOException {
        for (ICuboidWriter writer : cuboidWriters) {
            writer.flush();
        }

    }

    @Override
    public void close() throws IOException {
        for (ICuboidWriter writer : cuboidWriters) {
            writer.close();
        }

    }
}
