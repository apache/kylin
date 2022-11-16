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

package org.apache.kylin.measure.bitmap;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class RoaringBitmapCounterFactory implements BitmapCounterFactory, Serializable {
    public static final BitmapCounterFactory INSTANCE = new RoaringBitmapCounterFactory();

    private RoaringBitmapCounterFactory() {
    }

    @Override
    public BitmapCounter newBitmap() {
        return new RoaringBitmapCounter();
    }

    @Override
    public BitmapCounter newBitmap(long... values) {
        return new RoaringBitmapCounter(Roaring64NavigableMap.bitmapOf(values));
    }

    @Override
    public BitmapCounter newBitmap(long counter) {
        return new RoaringBitmapCounter(counter);
    }

    @Override
    public BitmapCounter newBitmap(ByteBuffer in) throws IOException {
        RoaringBitmapCounter counter = new RoaringBitmapCounter();
        counter.readFields(in);
        return counter;
    }
}
