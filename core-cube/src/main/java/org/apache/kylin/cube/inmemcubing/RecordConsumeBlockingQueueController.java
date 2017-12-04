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

package org.apache.kylin.cube.inmemcubing;

import java.util.concurrent.BlockingQueue;

public class RecordConsumeBlockingQueueController<T> extends ConsumeBlockingQueueController<T> {

    public final InputConverterUnit<T> inputConverterUnit;

    private RecordConsumeBlockingQueueController(InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input, int batchSize) {
        super(input, batchSize);
        this.inputConverterUnit = inputConverterUnit;
    }
   
    private T currentObject = null;
    private volatile boolean ifEnd = false;
    private volatile boolean cut = false;
    private long outputRowCountCut = 0L;

    @Override
    public boolean hasNext() {
        if (currentObject != null) {
            return hasNext(currentObject);
        }
        if (!super.hasNext()) {
            return false;
        }
        currentObject = super.next();
        return hasNext(currentObject);
    }

    @Override
    public T next() {
        if (ifEnd())
            throw new IllegalStateException();

        T result = currentObject;
        currentObject = null;
        return result;
    }

    public boolean ifEnd() {
        return ifEnd;
    }

    private boolean hasNext(T object) {
        if (inputConverterUnit.ifEnd(object)) {
            ifEnd = true;
            return false;
        }else if(cut){
            return false;
        }else if(inputConverterUnit.ifCut(object)){
            return false;
        }
        return true;
    }
    
    public static <T> RecordConsumeBlockingQueueController<T> getQueueController(InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input){
        return new RecordConsumeBlockingQueueController<>(inputConverterUnit, input, DEFAULT_BATCH_SIZE);
    }
    
    public static <T> RecordConsumeBlockingQueueController<T> getQueueController(InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input, int batchSize){
        return new RecordConsumeBlockingQueueController<>(inputConverterUnit, input, batchSize);
    }

    public void forceCutPipe() {
        cut = true;
        outputRowCountCut = getOutputRowCount();
    }

    public long getOutputRowCountAfterCut() {
        return getOutputRowCount() - outputRowCountCut;
    }
}
