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

package org.apache.kylin.util;

import java.util.BitSet;

import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoUtils {

    private static ThreadLocal<Kryo> _Kryo = new ThreadLocal<>();

    public static byte[] serialize(Object obj) {
        Kryo kryo = getKryo();
        Output output = new Output(1024, 8 * 1024 * 1024);
        kryo.writeObject(output, obj);
        return output.toBytes();
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz) {
        Kryo kryo = getKryo();
        Input input = new Input(bytes);
        return kryo.readObject(input, clazz);
    }

    public static <T> T copy(T origin, Class<T> clazz) {
        byte[] bytes = serialize(origin);
        return deserialize(bytes, clazz);
    }

    public static Kryo getKryo() {
        if (_Kryo.get() == null) {
            Kryo kryo = new Kryo();
            kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
            kryo.register(BitSet.class, new BitSetSerializer());
            _Kryo.set(kryo);
        }

        return _Kryo.get();
    }
}
