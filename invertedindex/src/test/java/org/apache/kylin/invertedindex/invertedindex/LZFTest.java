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

package org.apache.kylin.invertedindex.invertedindex;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

/**
 * Created by Hongbin Ma(Binmahone) on 2/6/15.
 */
public class LZFTest {
    @Test
    public void test() throws IOException {

        byte[] raw = new byte[] { 1, 2, 3, 3, 2, 23 };
        byte[] data = LZFEncoder.encode(raw);

        byte[] data2 = new byte[data.length * 2];
        java.lang.System.arraycopy(data, 0, data2, 0, data.length);
        ImmutableBytesWritable bytes = new ImmutableBytesWritable();
        bytes.set(data2, 0, data.length);

        try {
            byte[] uncompressed = LZFDecoder.decode(bytes.get(), bytes.getOffset(), bytes.getLength());
        } catch (IOException e) {
            throw new RuntimeException("LZF decode failure", e);
        }
    }
}
