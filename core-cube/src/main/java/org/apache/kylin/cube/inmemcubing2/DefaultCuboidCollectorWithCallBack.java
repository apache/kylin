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

package org.apache.kylin.cube.inmemcubing2;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.kylin.cube.inmemcubing.CuboidResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCuboidCollectorWithCallBack implements ICuboidCollectorWithCallBack{
    
    private static Logger logger = LoggerFactory.getLogger(DefaultCuboidCollectorWithCallBack.class);
    
    final ConcurrentNavigableMap<Long, CuboidResult> result = new ConcurrentSkipListMap<Long, CuboidResult>();
    final ICuboidResultListener listener;
    
    public DefaultCuboidCollectorWithCallBack(ICuboidResultListener listener){
        this.listener = listener;
    }

    @Override
    public void collectAndNotify(CuboidResult cuboidResult) {
        logger.info("collecting CuboidResult cuboid id:" + cuboidResult.cuboidId);
        result.put(cuboidResult.cuboidId, cuboidResult);
        if (listener != null) {
            listener.finish(cuboidResult);
        }
    }

    @Override
    public NavigableMap<Long, CuboidResult> getAllResult() {
        return result;
    }
}
