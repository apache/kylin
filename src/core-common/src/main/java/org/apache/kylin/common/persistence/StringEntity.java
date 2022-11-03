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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("serial")
public class StringEntity extends RootPersistentEntity {

    public static final Serializer<StringEntity> serializer = new Serializer<StringEntity>() {
        @Override
        public void serialize(StringEntity obj, DataOutputStream out) throws IOException {
            out.writeUTF(obj.str);
        }

        @Override
        public StringEntity deserialize(DataInputStream in) throws IOException {
            String str = in.readUTF();
            return new StringEntity(str);
        }
    };

    @Getter
    @Setter
    private String str;

    public StringEntity(String str) {
        this.str = str;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((str == null) ? 0 : str.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!(obj instanceof StringEntity))
            return false;
        return StringUtils.equals(this.str, ((StringEntity) obj).str);
    }

    @Override
    public String toString() {
        return str;
    }
}