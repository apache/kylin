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

import org.apache.kylin.dict.IDictionaryValueEnumerator;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

/**
 * Created by dongli on 10/28/15.
 */
public class ListDictionaryValueEnumerator implements IDictionaryValueEnumerator {
    ListIterator<byte[]> listIterator;

    public ListDictionaryValueEnumerator(List<byte[]> list) {
        listIterator = list.listIterator();
    }

    @Override
    public byte[] current() throws IOException {
        return listIterator.next();
    }

    @Override
    public boolean moveNext() throws IOException {
        return listIterator.hasNext();
    }

    @Override
    public void close() throws IOException {
    }
}
