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

package org.apache.kylin.common.persistence;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.kylin.common.util.JsonUtil;

/**
 * @author yangli9
 */
public class JsonSerializer<T extends RootPersistentEntity> implements Serializer<T> {

    Class<T> clz;
    boolean compact = false;

    public JsonSerializer(Class<T> clz) {
        this.clz = clz;
    }

    public JsonSerializer(Class<T> clz, boolean compact) {
        this.clz = clz;
        this.compact = compact;
    }

    @Override
    public T deserialize(DataInputStream in) throws IOException {
        return JsonUtil.readValue(in, clz);
    }

    @Override
    public void serialize(T obj, DataOutputStream out) throws IOException {
        if (compact)
            JsonUtil.writeValue(out, obj);
        else
            JsonUtil.writeValueIndent(out, obj);
    }
}
