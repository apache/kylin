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

package org.apache.kylin.common.util;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

public class SerializeToByteBuffer {

    public interface IWriter {
        void write(ByteBuffer byteBuffer) throws BufferOverflowException;
    }

    public static ByteBuffer retrySerialize(IWriter writer) {
        int bufferSize = BytesSerializer.SERIALIZE_BUFFER_SIZE;

        while (true) {
            try {
                ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
                writer.write(byteBuffer);
                return byteBuffer;
            } catch (BufferOverflowException boe) {
                System.out.println("Buffer size cannot hold the raw scans, resizing to 4 times : " + bufferSize);
                bufferSize *= 4;
            }
        }
    }
}
