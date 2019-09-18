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

package org.apache.kylin.stream.core.storage;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.model.stats.SegmentStoreStats;
import org.apache.kylin.stream.core.query.IStreamingGTSearcher;

public interface IStreamingSegmentStore extends IStreamingGTSearcher {
    void init();

    int addEvent(StreamingMessage event);

    default void addExternalDict(Map<TblColRef, Dictionary<String>> dictMap){}

    File getStorePath();

    void persist();

    /**
     * the latest store state, checkpoint implementation need this info
     * to save the store state
     * @return
     */
    Object checkpoint();

    void purge();

    void restoreFromCheckpoint(Object checkpoint);

    String getSegmentName();

    StreamingCubeSegment.State getSegmentState();

    void setSegmentState(StreamingCubeSegment.State state);

    SegmentStoreStats getStoreStats();

    void close() throws IOException;
}
