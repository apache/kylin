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

package org.apache.kylin.rest.service;

public class HBaseInfoUtil {
    
//    @SuppressWarnings("unused") // used by reflection
//    public static StorageResponse getHBaseInfo(String tableName, KylinConfig config) throws IOException {
//        if (!config.getStorageUrl().getScheme().equals("hbase"))
//            return null;
//
//        Connection conn = HBaseUnionUtil.getConnection(config, tableName);
//        StorageResponse hr = null;
//        long tableSize = 0;
//        int regionCount = 0;
//
//        HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(tableName, conn);
//        Map<byte[], Long> sizeMap = cal.getRegionSizeMap();
//
//        for (long s : sizeMap.values()) {
//            tableSize += s;
//        }
//
//        regionCount = sizeMap.size();
//
//        // Set response.
//        hr = new StorageResponse();
//        hr.setTableSize(tableSize);
//        hr.setRegionCount(regionCount);
//        return hr;
//    }
}
