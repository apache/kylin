/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming;

/**
 * Created by qianzhou on 2/15/15.
 */
public class Stream {

    private long offset;
    private byte[] rawData;

    public static final Stream EOF = new Stream(-1, new byte[0]);

    public Stream(long offset, byte[] rawData) {
        this.offset = offset;
        this.rawData = rawData;
    }

    public byte[] getRawData() {
        return rawData;
    }

    public long getOffset() {
        return offset;
    }
}
