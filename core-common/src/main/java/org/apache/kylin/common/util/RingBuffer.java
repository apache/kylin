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

package org.apache.kylin.common.util;

/**
 * @author zhaoliu4
 * @date 2022/11/3
 */
public class RingBuffer {
    private final byte[] bytes;

    /**
     * next write position
     */
    private int writePos;

    /**
     * number of bytes has stored
     */
    private int size;

    private RingBuffer(int capacity) {
        this.bytes = new byte[capacity];
    }

    public static RingBuffer allocate(int capacity) {
        return new RingBuffer(capacity);
    }

    public RingBuffer put(byte[] src) {
        for (int i = 0; i < src.length; i++) {
            if (writePos >= bytes.length) {
                // reset, turn back continue to write
                writePos = 0;
            }
            bytes[writePos++] = src[i];
            size = size < bytes.length ? size + 1 : size;
        }
        return this;
    }

    public byte[] get() {
        byte[] res;
        if (size == bytes.length && writePos < size) {
            // occur turn back
            res = new byte[size];
            System.arraycopy(bytes, writePos, res, 0, size - writePos);
            System.arraycopy(bytes, 0, res, size - writePos, writePos);
        } else {
            res = new byte[writePos];
            System.arraycopy(bytes, 0, res, 0, writePos);
        }
        return res;
    }
}
