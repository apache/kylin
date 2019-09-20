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
package org.apache.kylin.stream.core.dict;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Dictionary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

/**
 * Encode string into integer in real-time and distributed way.
 */
public class StreamingDistributedDictionary extends Dictionary<String> {

    private ByteArray cubeColumn;
    private transient StreamingDictionaryClient streamingDictionaryClient;

    public StreamingDistributedDictionary(String cubeColumn, StreamingDictionaryClient streamingDictionaryClient) {
        this.streamingDictionaryClient = streamingDictionaryClient;
        this.cubeColumn = new ByteArray(cubeColumn.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int getMinId() {
        return Integer.MIN_VALUE;
    }

    @Override
    public int getMaxId() {
        return Integer.MAX_VALUE;
    }

    @Override
    public int getSizeOfId() {
        return 0;
    }

    @Override
    public int getSizeOfValue() {
        return 0;
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        return false;
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        return streamingDictionaryClient.encode(cubeColumn, value);
    }

    @Override
    protected String getValueFromIdImpl(int id) {
        return "";
    }

    @Override
    public void dump(PrintStream out) {
        throw new UnsupportedOperationException("Do not dump me.");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException("Do not copy me.");
    }

    @Override
    public void readFields(DataInput in) {
        throw new UnsupportedOperationException("Do not read me.");
    }
}
