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
package org.apache.kylin.common;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.CompressionUtils;
import org.junit.Test;

import java.util.Random;

/**
 * KylinCodecTest
 * created by haihuang.hhl @2020/1/9
 */
public class KylinCodecTest {
    public static byte[] mockData(int loop) {
        int base = 1000;
        int num = (int) (base * Math.pow(10, loop));
        byte[] data = new byte[num];
        Random random = new Random();
        for (int i = 0; i < num; i++) {
            data[i] = (byte) random.nextInt(2);
        }
        return data;
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i <= 6; i++) {
            byte[] data = mockData(i);
            String[] algorithms = new String[]{"lz4", "zstd", ""};
            for (String algo : algorithms) {
                long start = System.currentTimeMillis();
                byte[] compressed = CompressionUtils.compress(data, algo);
                long start_c = System.currentTimeMillis();
                byte[] decompressed = CompressionUtils.decompress(compressed, algo);
                long end_dc = System.currentTimeMillis();
                assert Bytes.compareTo(data, decompressed) == 0;
                if ("".equals(algo)) {
                    algo = "java-zip";
                }
                double compress_ratio = 0;
                if (compressed.length > 0) {
                    compress_ratio = (double) data.length / compressed.length;
                }
                System.out.println("algo " + algo + " original size:" + data.length +
                        " after:" + compressed.length + " compress ratio:" + String.format("%.2f", compress_ratio)
                        + " compress time cost:" +
                        (start_c - start) + " decompress time cost:" + (end_dc - start_c));
            }
        }

    }


}
