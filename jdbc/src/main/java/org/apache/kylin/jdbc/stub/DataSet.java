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

package org.apache.kylin.jdbc.stub;

import java.util.List;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Data set wrapper.
 * 
 * @author xduo
 * 
 */
public class DataSet<E> {

    private final List<ColumnMetaData> meta;

    private final Enumerator<E> enumerator;

    /**
     * @param meta
     * @param enumerator
     */
    public DataSet(List<ColumnMetaData> meta, Enumerator<E> enumerator) {
        this.meta = meta;
        this.enumerator = enumerator;
    }

    public List<ColumnMetaData> getMeta() {
        return meta;
    }

    public Enumerator<E> getEnumerator() {
        return enumerator;
    }

}
