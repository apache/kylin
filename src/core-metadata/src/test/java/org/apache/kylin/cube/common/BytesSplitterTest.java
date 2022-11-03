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

package org.apache.kylin.cube.common;

import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import org.apache.kylin.common.util.BytesSplitter;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
public class BytesSplitterTest {

    @Test
    public void test() {
        BytesSplitter bytesSplitter = new BytesSplitter(10, 15);
        byte[] input = "2013-02-17Collectibles".getBytes(Charset.defaultCharset());
        bytesSplitter.split(input, input.length, (byte) 127);

        assertEquals(2, bytesSplitter.getBufferSize());
        assertEquals("2013-02-17", new String(bytesSplitter.getSplitBuffers()[0].value, 0,
                bytesSplitter.getSplitBuffers()[0].length, Charset.defaultCharset()));
        assertEquals("Collectibles", new String(bytesSplitter.getSplitBuffers()[1].value, 0,
                bytesSplitter.getSplitBuffers()[1].length, Charset.defaultCharset()));
    }

    @Test
    public void testNullValue() {
        BytesSplitter bytesSplitter = new BytesSplitter(10, 15);
        byte[] input = "2013-02-17Collectibles".getBytes(Charset.defaultCharset());
        bytesSplitter.split(input, input.length, (byte) 127);

        assertEquals(3, bytesSplitter.getBufferSize());
        assertEquals("2013-02-17", new String(bytesSplitter.getSplitBuffers()[0].value, 0,
                bytesSplitter.getSplitBuffers()[0].length, Charset.defaultCharset()));
        assertEquals("", new String(bytesSplitter.getSplitBuffers()[1].value, 0,
                bytesSplitter.getSplitBuffers()[1].length, Charset.defaultCharset()));
        assertEquals("Collectibles", new String(bytesSplitter.getSplitBuffers()[2].value, 0,
                bytesSplitter.getSplitBuffers()[2].length, Charset.defaultCharset()));
    }
}
