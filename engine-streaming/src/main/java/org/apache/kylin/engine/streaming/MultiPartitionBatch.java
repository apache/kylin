/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */
package org.apache.kylin.engine.streaming;

import com.google.common.base.Preconditions;

import java.util.AbstractList;
import java.util.List;

/**
 */
public class MultiPartitionBatch extends AbstractList<StreamingMessage> implements StreamingBatch {
    
    private final List<SinglePartitionBatch> partitionBatchList;
    
    private final int size;

    public MultiPartitionBatch(List<SinglePartitionBatch> partitionBatchList) {
        this.partitionBatchList = Preconditions.checkNotNull(partitionBatchList);
        this.size = calculateSize();
    }
    
    private int calculateSize() {
        int result = 0;
        for (SinglePartitionBatch singlePartitionBatch : partitionBatchList) {
            result += singlePartitionBatch.size();
        }
        return result;
    }

    @Override
    public StreamingMessage get(int index) {
        Preconditions.checkElementIndex(index, size);
        for (int i = 0; i < partitionBatchList.size(); ++i) {
            final SinglePartitionBatch batch = partitionBatchList.get(i);
            if (index < batch.size()) {
                return batch.get(index);
            } else {
                index = index - batch.size();
            }
        }
        throw new IllegalStateException("index = " + index);
    }

    @Override
    public int size() {
        return size;
    }
}
