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

package org.apache.kylin.measure.bitmap.intersect;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 * @Deprecated This class will no longer be used， but leave it in order to make the framework work.
 */
@Deprecated
public class IntersectSerializer extends DataTypeSerializer<IntersectBitmapCounter> {

    private DataTypeSerializer<BitmapCounter> defaultSerializer;

    // called by reflection
    public IntersectSerializer(DataType type) {
        defaultSerializer = new BitmapSerializer(type);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return defaultSerializer.peekLength(in);
    }

    @Override
    public int maxLength() {
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return defaultSerializer.getStorageBytesEstimate();
    }

    @Override
    public void serialize(IntersectBitmapCounter value, ByteBuffer out) {
        Map<Object, BitmapCounter> map = value.getMap();
        out.putInt(map.size());
        for (Map.Entry<Object, BitmapCounter> entry : map.entrySet()) {
            byte[] src = Bytes.toBytes(entry.getKey().toString());
            out.putInt(src.length);
            out.put(src);
            defaultSerializer.serialize(entry.getValue(), out);
        }
    }

    @Override
    public IntersectBitmapCounter deserialize(ByteBuffer in) {
        IntersectBitmapCounter intersectBitmapCounter = new IntersectBitmapCounter();
        Map<Object, BitmapCounter> map = intersectBitmapCounter.getMap();
        int anInt = in.getInt();
        for (int i = 0; i < anInt; i++) {
            int keyLength = in.getInt();
            byte[] bytes = new byte[keyLength];
            in.get(bytes, 0, keyLength);
            String s = new String(bytes, Charset.defaultCharset());
            BitmapCounter deserialize = defaultSerializer.deserialize(in);
            map.put(s, deserialize);
        }

        return intersectBitmapCounter;
    }

    @Override
    public boolean supportDirectReturnResult() {
        return true;
    }

    @Override
    public ByteBuffer getFinalResult(ByteBuffer in) {
        throw new UnsupportedOperationException();
    }
}
