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

package org.apache.kylin.metadata.model;

import java.util.Set;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

public class ComputedColumnDescTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void simpleParserCheckTestSuccess1() {
        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + b.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail1() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                "Can’t recognize column \"C.Y\". Please use \"TABLE_ALIAS.COLUMN\" to reference a column.");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("a.x + c.y", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail2() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: SUM");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("sum(a.x) * 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail3() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MIN");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("min(a.x) + 10", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail4() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: MAX");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("max(a.x + b.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail5() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(*)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail6() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail7() {
        //value window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: LEAD");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("lead(a.x,1) over (order by a.y)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail8() {
        //ranking window function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: NTILE");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("ntile(5) over(order by a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail9() {
        //aggregate function
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: AVG");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) ", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail10() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain keyword AS");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("avg(a.x) over (partition by a.y) as xxx", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFail11() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: COUNT");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("count(distinct a.x)", aliasSet);
    }

    @Test
    public void simpleParserCheckTestFailUDAF() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Computed column expression should not contain any aggregate functions: CORR");

        ComputedColumnDesc cc = new ComputedColumnDesc();
        Set<String> aliasSet = Sets.newHashSet("A", "B");
        cc.simpleParserCheck("corr(a.x, a.b)", aliasSet);
    }
}
