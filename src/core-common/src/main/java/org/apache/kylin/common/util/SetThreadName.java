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

import java.io.Closeable;
import java.util.Locale;

/**
 *
 * An utility that makes it easy to change current thread name in a try-with-resource block.
 *
 * <p> Example:
 *
 * <pre>
 *     // old thread name
 *     try (SetThreadName ignored = new SetThreadName(format, arg1, arg2)) {
 *         // new thread name
 *     }
 *     // old thread name
 * </pre>
 */
public class SetThreadName implements Closeable {
    private final String originThreadName;

    public SetThreadName(String format, Object... args) {
        originThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(String.format(Locale.ROOT, format, args) + "-" + Thread.currentThread().getId());
    }

    @Override
    public void close() {
        Thread.currentThread().setName(originThreadName);
    }
}
