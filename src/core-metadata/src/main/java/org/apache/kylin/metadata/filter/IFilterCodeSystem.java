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

package org.apache.kylin.metadata.filter;

import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * Decides how constant values are coded and compared.
 *
 * TupleFilter are involved in both query engine and coprocessor. In query engine, the values are strings.
 * In coprocessor, the values are dictionary IDs.
 *
 * The type parameter is the java type of code, which should be bytes. However some legacy implementation
 * stores code as String.
 *
 * @author yangli9
 */
public interface IFilterCodeSystem<T> extends Comparator<T> {

    /** if given code represents the NULL value */
    boolean isNull(T code);

    /** compare two values by their codes */
    // int compare(T code1, T code2);

    /** write code to buffer */
    void serialize(T code, ByteBuffer buf);

    /** read code from buffer */
    T deserialize(ByteBuffer buf);

}
