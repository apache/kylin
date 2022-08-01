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

package org.apache.kylin.measure.dim;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.util.KryoUtils;

public class DimCountDistincSerializer extends DataTypeSerializer<DimCountDistinctCounter> {
    // called by reflection
    public DimCountDistincSerializer(DataType type) {
    }

    @Override
    public void serialize(DimCountDistinctCounter value, ByteBuffer out) {
        byte[] serialize = KryoUtils.serialize(value.getContainer());
        out.putInt(4 + 4 + 4 + serialize.length);
        out.putInt(value.getMAX_CARD());
        out.putInt(serialize.length);
        out.put(serialize);
    }

    @Override
    public DimCountDistinctCounter deserialize(ByteBuffer in) {
        int totalLength = in.getInt();
        int maxCard = in.getInt();
        int arrayLength = in.getInt();
        byte[] data = new byte[arrayLength];
        in.get(data);
        return new DimCountDistinctCounter(KryoUtils.deserialize(data, Set.class), maxCard);
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int ret = in.getInt();
        in.position(mark);
        return ret;
    }

    @Override
    public int maxLength() {
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        return 1024;
    }
}
