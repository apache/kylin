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

package org.apache.kylin.storage.hbase.common.coprocessor;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.List;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.junit.Test;

/**
 * @author xjiang
 * 
 */
public class FilterEvaluateTest extends FilterBaseTest {

    @Test
    public void testEvaluate00() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildEQCompareFilter(groups, 0);

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[0]);
    }

    @Test
    public void testEvaluate01() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildEQCompareFilter(groups, 1);

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[1]);
    }

    @Test
    public void testEvaluate02() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildOrFilter(groups);

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[0] + matcheCounts[1] - matcheCounts[2]);
    }

    @Test
    public void testEvaluate03() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildAndFilter(groups);

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[2]);
    }

    @Test
    public void testEvaluate04() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildCompareCaseFilter(groups, "0");

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[2]);
    }

    @Test
    public void testEvaluate05() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildCompareCaseFilter(groups, "1");

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[0] - matcheCounts[2]);
    }

    @Test
    public void testEvaluate06() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildCompareCaseFilter(groups, "2");

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 1;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, matcheCounts[1] - matcheCounts[2]);
    }

    @Test
    public void testEvaluate07() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildCompareCaseFilter(groups, "3");

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, 0);
    }

    @Test
    public void testEvaluate08() {
        List<TblColRef> groups = buildGroups();
        TupleFilter filter = buildCompareCaseFilter(groups, "4");

        byte[] bytes = TupleFilterSerializer.serialize(filter, CS);
        TupleFilter newFilter = TupleFilterSerializer.deserialize(bytes, CS);

        int number = 10000;
        int[] matcheCounts = new int[] { 0, 0, 0 };
        Collection<Tuple> tuples = generateTuple(number, groups, matcheCounts);
        int match = evaluateTuples(tuples, newFilter);

        assertEquals(match, number - matcheCounts[0] - matcheCounts[1] + matcheCounts[2]);
    }

}
