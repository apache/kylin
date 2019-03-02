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

package org.apache.kylin.stream.core.source;

import java.util.List;
import java.util.Map;

import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.consumer.IStreamingConnector;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;

public interface IStreamingSource {
    /**
     * load the streaming source information for the cube
     * @param cubeName
     * @return
     */
    StreamingTableSourceInfo load(String cubeName);

    /**
     * fetch message template for specified streaming source
     * @param streamingSourceConfig
     * @return
     */
    String getMessageTemplate(StreamingSourceConfig streamingSourceConfig);

    /**
     * create streaming connector
     * @param cubeName
     * @param partitions
     * @param startProtocol
     * @param cubeSegmentManager
     * @return
     */
    IStreamingConnector createStreamingConnector(String cubeName, List<Partition> partitions, ConsumerStartProtocol startProtocol,
            StreamingSegmentManager cubeSegmentManager);


    ISourcePositionHandler getSourcePositionHandler();

    /**
     * Calculate the lag from latest offset to current offset
     */
    Map<Integer, Long> calConsumeLag(String cubeName, ISourcePosition currentPosition);
}
