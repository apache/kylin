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

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;

public class RecordConsumeBlockingQueueController<T> extends ConsumeBlockingQueueController<T> {

    public final InputConverterUnit<T> inputConverterUnit;

    private RecordConsumeBlockingQueueController(InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input,
            int batchSize) {
        super(input, batchSize);
        this.inputConverterUnit = inputConverterUnit;
    }

    private T currentObject = null;
    private volatile boolean ifEnd = false;

    @Override
    public boolean hasNext() { // should be idempotent
        if (ifEnd) {
            return false;
        }
        if (currentObject != null) {
            return true;
        }
        if (!super.hasNext()) {
            return false;
        }
        currentObject = super.next();

        if (inputConverterUnit.ifEnd(currentObject)) {
            ifEnd = true;
            return false;
        } else if (inputConverterUnit.ifCut(currentObject)) {
            currentObject = null;
            hasNext();
            return false;
        }
        return true;
    }

    @Override
    public T next() {
        if (ifEnd() || currentObject == null)
            throw new NoSuchElementException();

        T result = currentObject;
        currentObject = null;
        return result;
    }

    public boolean ifEnd() {
        return ifEnd;
    }

    public static <T> RecordConsumeBlockingQueueController<T> getQueueController(
            InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input) {
        return new RecordConsumeBlockingQueueController<>(inputConverterUnit, input, DEFAULT_BATCH_SIZE);
    }

    public static <T> RecordConsumeBlockingQueueController<T> getQueueController(
            InputConverterUnit<T> inputConverterUnit, BlockingQueue<T> input, int batchSize) {
        return new RecordConsumeBlockingQueueController<>(inputConverterUnit, input, batchSize);
    }
}
