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

package org.apache.kylin.dict;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;

/**
 */
public final class DictionarySerializer {

    private DictionarySerializer() {
    }

    public static Dictionary<?> deserialize(InputStream inputStream) {
        try {
            final DataInputStream dataInputStream = new DataInputStream(inputStream);
            final String type = dataInputStream.readUTF();
            final Dictionary<?> dictionary = ClassUtil.forName(type, Dictionary.class).newInstance();
            dictionary.readFields(dataInputStream);
            return dictionary;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Dictionary<?> deserialize(ByteArray dictBytes) {
        return deserialize(new ByteArrayInputStream(dictBytes.array(), dictBytes.offset(), dictBytes.length()));
    }

    public static void serialize(Dictionary<?> dict, OutputStream outputStream) {
        try {
            DataOutputStream out = new DataOutputStream(outputStream);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteArray serialize(Dictionary<?> dict) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            out.writeUTF(dict.getClass().getName());
            dict.write(out);
            return new ByteArray(baos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
