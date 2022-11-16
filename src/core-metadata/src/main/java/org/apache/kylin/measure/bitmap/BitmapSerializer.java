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
import java.nio.ByteBuffer;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class BitmapSerializer extends DataTypeSerializer<BitmapCounter> {
    private static final BitmapCounterFactory factory = RoaringBitmapCounterFactory.INSTANCE;
    private static final BitmapCounter DELEGATE = factory.newBitmap();

    private static final int IS_RESULT_FLAG = 1;
    private static final int RESULT_SIZE = 12;

    // called by reflection
    public BitmapSerializer(DataType type) {
    }

    @Override
    public void serialize(BitmapCounter value, ByteBuffer out) {
        try {
            value.write(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BitmapCounter deserialize(ByteBuffer in) {
        try {
            return factory.newBitmap(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int peekLength(ByteBuffer in) {
        ByteBuffer buffer = in.slice();
        if (buffer.getInt(0) == IS_RESULT_FLAG) {
            return RESULT_SIZE;
        } else {
            return DELEGATE.peekLength(in);
        }
    }

    @Override
    public int maxLength() {
        // the bitmap is non-fixed length, and we just assume 8MB here, maybe change it later
        // some statistics for bitmap: 
        // 1 million distinct keys takes about 2MB storage
        // 5 million takes 10MB
        // 10 million takes 12MB
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        // It's difficult to decide the size before data was ingested, comparing with HLLCounter(16) as 64KB, here is assumption
        return 8 * 1024;
    }

    @Override
    public boolean supportDirectReturnResult() {
        return true;
    }

    @Override
    public ByteBuffer getFinalResult(ByteBuffer in) {
        ByteBuffer out = ByteBuffer.allocate(RESULT_SIZE);
        try {
            BitmapCounter counter = factory.newBitmap(in);
            out.putInt(IS_RESULT_FLAG);
            out.putLong(counter.getCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        out.flip();
        return out;
    }
}
