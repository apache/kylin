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

package org.apache.kylin.measure.bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * An implementation-agnostic bitmap type.
 */
public interface BitmapCounter extends Iterable<Integer> {
    /**
     * @return cardinality of the bitmap
     */
    long getCount();

    /**
     * @return estimated memory footprint of this counter
     */
    int getMemBytes();

    /**
     * @return a iterator of the ints stored in this counter.
     */
    Iterator<Integer> iterator();

    /**
     * Serialize this counter. The current counter is not modified.
     */
    void serialize(ByteBuffer out) throws IOException;

    /**
     * Deserialize a counter from its serialized form.
     * <p> After deserialize, any changes to `in` should not affect the returned counter.
     */
    BitmapCounter deserialize(ByteBuffer in) throws IOException;

    /**
     * @return size of the counter stored in the current position of `in`.
     * The position field must not be modified.
     */
    int peekLength(ByteBuffer in);
}
