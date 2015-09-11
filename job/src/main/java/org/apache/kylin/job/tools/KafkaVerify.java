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

package org.apache.kylin.job.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;

/**
 * only for verify kylin streaming's correctness by comparing to data in original kafka topic
 */
public class KafkaVerify {

    public static void main(String[] args) throws IOException {

        System.out.println("start");
        
        ObjectMapper mapper = new ObjectMapper();
        JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(String.class));

        long start = Long.valueOf(args[0]);
        long end = Long.valueOf(args[1]);
        long interval = Long.valueOf(args[2]);
        int bucket = (int) ((end - start + interval - 1) / interval);
        
        long qtySum[] = new long[bucket];
        long qtyTotal = 0;
        long counts[] = new long[bucket];
        long countTotal = 0;
        long processed = 0;
        long minOffset = -1;
        long maxOffset = -1;

        try (BufferedReader br = new BufferedReader(new FileReader(new File(args[3])))) {
            String s;
            while ((s = br.readLine()) != null) {
                // process the line.
                if (++processed % 10000 == 1) {
                    System.out.println("processing " + processed);
                }

                Map<String, String> root = mapper.readValue(s, mapType);
                String tsStr = root.get("sys_ts");

                if (StringUtils.isEmpty(tsStr)) {
                    continue;
                }
                long ts = Long.valueOf(tsStr);
                if (ts < start || ts >= end) {
                    continue;
                }

                if (minOffset == -1) {
                    minOffset = processed - 1;
                }
                maxOffset = processed - 1;

                long qty = Long.valueOf(root.get("qty"));
                int index = (int) ((ts - start) / interval);
                qtySum[index] += qty;
                qtyTotal += qty;
                counts[index]++;
                countTotal++;
            }
        }

        System.out.println("qty sum is " + Arrays.toString(qtySum));
        System.out.println("qty total is " + qtyTotal);
        System.out.println("count is " + Arrays.toString(counts));
        System.out.println("count total is " + countTotal);
        System.out.println("first processed is " + minOffset);
        System.out.println("last processed is " + maxOffset);
    }
}
