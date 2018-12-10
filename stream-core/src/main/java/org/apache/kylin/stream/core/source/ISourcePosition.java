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

import java.util.Map;

public interface ISourcePosition {
    void update(IPartitionPosition point);

    /**
     * update the partition position info into the position
     * when the partition position is not exist in the position.
     * @param partitionPosition
     */
    void updateWhenPartitionNotExist(IPartitionPosition partitionPosition);

    /**
     * advance the source position
     * @return
     */
    ISourcePosition advance();

    Map<Integer, IPartitionPosition> getPartitionPositions();

    void copy(ISourcePosition other);

    interface IPartitionPosition extends Comparable<IPartitionPosition> {
        int getPartition();
    }
}
