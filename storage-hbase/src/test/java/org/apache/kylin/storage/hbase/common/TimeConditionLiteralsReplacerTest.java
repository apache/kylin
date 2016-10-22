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

package org.apache.kylin.storage.hbase.common;

import org.apache.kylin.dict.DictCodeSystem;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TimeConditionLiteralsReplacer;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.common.coprocessor.FilterBaseTest;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class TimeConditionLiteralsReplacerTest extends FilterBaseTest {
    @Test
    public void basicTest() {
        TableDesc t1 = TableDesc.mockup("DEFAULT.TEST_KYLIN_FACT");
        TblColRef column = TblColRef.mockup(t1, 2, "CAL_DT", "date");

        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = null;
        constantFilter = new ConstantTupleFilter("946684800000");
        compareFilter.addChild(constantFilter);

        TimeConditionLiteralsReplacer filterDecorator = new TimeConditionLiteralsReplacer(compareFilter);
        byte[] bytes = TupleFilterSerializer.serialize(compareFilter, filterDecorator, DictCodeSystem.INSTANCE);
        CompareTupleFilter compareTupleFilter = (CompareTupleFilter) TupleFilterSerializer.deserialize(bytes, DictCodeSystem.INSTANCE);
        Assert.assertEquals("2000-01-01", compareTupleFilter.getFirstValue());
    }
}
