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

package org.apache.kylin.query.enumerator;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OLAPQuery extends AbstractEnumerable<Object[]> implements Enumerable<Object[]> {

    private static final Logger logger = LoggerFactory.getLogger(OLAPQuery.class);

    public enum EnumeratorTypeEnum {
        OLAP, //finish query with Cube or II, or a combination of both
        LOOKUP_TABLE, //using a snapshot of lookup table
        HIVE, //using hive
        COL_DICT // using a column's dictionary
    }

    private final DataContext optiqContext;
    private final EnumeratorTypeEnum type;
    private final int contextId;

    public OLAPQuery(DataContext optiqContext, EnumeratorTypeEnum type, int ctxId) {
        this.optiqContext = optiqContext;
        this.type = type;
        this.contextId = ctxId;

        QueryContextFacade.current().addContext(ctxId, type.toString(),
                type == EnumeratorTypeEnum.OLAP);
    }

    public OLAPQuery(EnumeratorTypeEnum type, int ctxSeq) {
        this(null, type, ctxSeq);
    }

    public Enumerator<Object[]> enumerator() {
        OLAPContext olapContext = OLAPContext.getThreadLocalContextById(contextId);
        switch (type) {
        case OLAP:
            return BackdoorToggles.getPrepareOnly() ? new EmptyEnumerator()
                    : new OLAPEnumerator(olapContext, optiqContext);
        case LOOKUP_TABLE:
            return BackdoorToggles.getPrepareOnly() ? new EmptyEnumerator() : new LookupTableEnumerator(olapContext);
        case COL_DICT:
            return BackdoorToggles.getPrepareOnly() ? new EmptyEnumerator() : new DictionaryEnumerator(olapContext);
        case HIVE:
            return BackdoorToggles.getPrepareOnly() ? new EmptyEnumerator() : new HiveEnumerator(olapContext);
        default:
            throw new IllegalArgumentException("Wrong type " + type + "!");
        }
    }

    public static class EmptyEnumerator implements Enumerator<Object[]> {
        
        public EmptyEnumerator() {
            logger.debug("Using empty enumerator");
        }

        @Override
        public void close() {
        }

        @Override
        public Object[] current() {
            return null;
        }

        @Override
        public boolean moveNext() {
            return false;
        }

        @Override
        public void reset() {
        }
    }
}
