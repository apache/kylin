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

package org.apache.kylin.stream.core.storage.columnar;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.stream.core.model.StreamingMessage;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.stream.core.source.ISourcePosition.IPartitionPosition;

public class StreamingDataSimulator {
    private Map<String, Integer> cardinalityMap;
    private int eventCntPerMin = 10000;
    private Random random = new Random();

    public StreamingDataSimulator() {
        this.cardinalityMap = getDefaultCardinalityMap();
    }
    public StreamingDataSimulator(Map<String, Integer> cardinalityMap) {
        this.cardinalityMap = cardinalityMap;
    }
    public StreamingDataSimulator(Map<String, Integer> cardinalityMap, int eventCntPerMin) {
        this.cardinalityMap = cardinalityMap;
        this.eventCntPerMin = eventCntPerMin;
    }

    public Iterator<StreamingMessage> simulate(final int eventCnt, final long startTime) {
        return new Iterator<StreamingMessage>() {
            int currIdx = 0;
            long time = startTime;
            Map<String, Object> params = Maps.newHashMap();

            @Override
            public boolean hasNext() {
                return currIdx < eventCnt;
            }

            @Override
            public StreamingMessage next() {
                List<String> data;
                int id = currIdx;
                data = simulateData(id, time, cardinalityMap);
                StreamingMessage message = new StreamingMessage(data, new IPartitionPosition() {
                    @Override
                    public int getPartition() {
                        return 0;
                    }

                    @Override
                    public int compareTo(IPartitionPosition o) {
                        return 0;
                    }
                }, time, params);
                currIdx++;
                if (currIdx % eventCntPerMin == 0) {
                    time = time + 60000;
                }
                return message;
            }

            @Override
            public void remove() {

            }
        };
    }

    private List<String> simulateData(int i, long time, Map<String, Integer> cardinalityMap) {
        List<String> data = Lists.newArrayList();
        data.add("SITE" + i % cardinalityMap.get("SITE"));//site
        data.add("ITM" + getFixLenID(10, i % cardinalityMap.get("ITM")));//item
        data.add(String.valueOf(TimeUtil.getDayStart(time)));
        data.add(String.valueOf(TimeUtil.getHourStart(time)));// hour start
        data.add(String.valueOf(TimeUtil.getMinuteStart(time)));// minute start
        data.add(String.valueOf(random.nextDouble() * 1000.0));//gmv
        //itm cnt
        if (i % 2 == 0) {
            data.add("2");
        } else {
            data.add("1");
        }

        return data;
    }

    private String getFixLenID(int fixLen, int id) {
        StringBuilder result = new StringBuilder();
        String idStr = String.valueOf(id);
        int more = fixLen - idStr.length();
        if (more > 0) {
            for (int i = 0; i < more; i++) {
                result.append("0");
            }
        }
        result.append(idStr);
        return result.toString();
    }

    public static Map<String, Integer> getDefaultCardinalityMap() {
        Map<String, Integer> result = new HashMap();

        result.put("SITE", 10);
        result.put("ITM", Integer.MAX_VALUE);
        return result;
    }

    public static void main(String[] args) {
        StreamingDataSimulator simulator = new StreamingDataSimulator();
        System.out.println(simulator.getFixLenID(10, 100));
    }
}
