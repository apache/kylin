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

package org.apache.kylin.measure.stddev;

import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class StdDevSerializer extends DataTypeSerializer<StdDevCounter> {

    public StdDevSerializer(DataType type) {
    }

    @Override
    public void serialize(StdDevCounter value, ByteBuffer out) {
        value.writeRegisters(out);
    }

    private StdDevCounter current() {
        StdDevCounter stdDevCounter = (StdDevCounter) current.get();
        if (stdDevCounter == null) {
            stdDevCounter = new StdDevCounter();
            current.set(stdDevCounter);
        }
        return stdDevCounter;
    }

    @Override
    public StdDevCounter deserialize(ByteBuffer in) {
        StdDevCounter stdDevCounter = current();
        stdDevCounter.readRegisters(in);
        return stdDevCounter;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return current().sizeOfBytes();
    }

    @Override
    public int maxLength() {
        return current().sizeOfBytes();
    }

    @Override
    public int getStorageBytesEstimate() {
        return current().sizeOfBytes();
    }

}