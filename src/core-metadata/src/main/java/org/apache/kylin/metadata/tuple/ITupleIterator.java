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

package org.apache.kylin.metadata.tuple;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author xjiang
 *
 */
public interface ITupleIterator extends Iterator<ITuple> {
    ITupleIterator EMPTY_TUPLE_ITERATOR = new ITupleIterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public ITuple next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {

        }

        @Override
        public void close() {
        }
    };

    void close();

    /**
     * if hasNext() returns false because there's no more data, return true
     * if hasNext() returns false because limits or threshold, return false
     * if hasNext() returns true, throw IllegalStateException
     * @return
     */
    //public boolean isDrained();
}
