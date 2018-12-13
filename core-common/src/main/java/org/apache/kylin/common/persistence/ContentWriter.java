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

package org.apache.kylin.common.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

abstract public class ContentWriter {

    public static ContentWriter create(byte[] data) {
        return create(new ByteArrayInputStream(data));
    }

    public static ContentWriter create(final InputStream is) {
        return new ContentWriter() {
            @Override
            void write(DataOutputStream out) throws IOException {
                int n = IOUtils.copy(is, out);
                bytesWritten += (n < 0 ? Integer.MAX_VALUE : n);
            }
        };
    }

    public static <T extends RootPersistentEntity> ContentWriter create(final T obj, final Serializer<T> serializer) {
        return new ContentWriter() {
            @Override
            void write(DataOutputStream out) throws IOException {
                int pos = out.size();
                serializer.serialize(obj, out);
                bytesWritten += (out.size() - pos);
            }
        };
    }

    private boolean isBigContent = false;
    protected long bytesWritten = 0;

    abstract void write(DataOutputStream out) throws IOException;

    public void markBigContent() {
        isBigContent = true;
    }

    public boolean isBigContent() {
        return isBigContent;
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    public byte[] extractAllBytes() throws IOException {
        try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(bout)) {
            write(dout);
            dout.flush();
            return bout.toByteArray();
        }
    }
}
