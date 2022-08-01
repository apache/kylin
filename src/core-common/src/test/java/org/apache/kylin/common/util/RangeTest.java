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

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;

/**
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class RangeTest {

    @Test
    public void extremeCase() {
        Range r1 = Range.all();
        Range r2 = Range.all();

        Range a = Range.closedOpen(2, 5);

        Assert.assertTrue(RangeUtil.remove(r1, r2).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, a).equals(Lists.newArrayList(Range.lessThan(2), Range.atLeast(5))));
    }

    @Test
    public void testClosed() {

        Range anull = Range.closedOpen(0, 0);

        Range r1 = Range.closed(2, 5);

        Range a1 = Range.open(1, 2);
        Range a2 = Range.open(1, 3);
        Range a3 = Range.open(1, 5);
        Range a4 = Range.open(1, 6);
        Range a5 = Range.open(6, 7);
        Range a6 = Range.open(5, 7);
        Range a7 = Range.open(4, 7);
        Range a8 = Range.open(2, 7);
        Range a9 = Range.open(1, 7);

        Range b1 = Range.closed(1, 2);
        Range b2 = Range.closed(1, 3);
        Range b3 = Range.closed(1, 5);
        Range b4 = Range.closed(1, 6);
        Range b5 = Range.closed(6, 7);
        Range b6 = Range.closed(5, 7);
        Range b7 = Range.closed(4, 7);
        Range b8 = Range.closed(2, 7);
        Range b9 = Range.closed(1, 7);

        Range c1 = Range.open(2, 3);
        Range c2 = Range.open(3, 4);
        Range c3 = Range.open(4, 5);

        Range d1 = Range.closed(2, 3);
        Range d2 = Range.closed(3, 4);
        Range d3 = Range.closed(4, 5);

        Assert.assertTrue(RangeUtil.remove(r1, anull).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a1).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a2).equals(Lists.newArrayList(Range.closed(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, a3).equals(Lists.newArrayList(Range.closed(5, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, a4).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, a5).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a6).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a7).equals(Lists.newArrayList(Range.closed(2, 4))));
        Assert.assertTrue(RangeUtil.remove(r1, a8).equals(Lists.newArrayList(Range.closed(2, 2))));
        Assert.assertTrue(RangeUtil.remove(r1, a9).equals(Lists.newArrayList()));

        Assert.assertTrue(RangeUtil.remove(r1, b1).equals(Lists.newArrayList(Range.openClosed(2, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b2).equals(Lists.newArrayList(Range.openClosed(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b3).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b4).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b5).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, b6).equals(Lists.newArrayList(Range.closedOpen(2, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b7).equals(Lists.newArrayList(Range.closedOpen(2, 4))));
        Assert.assertTrue(RangeUtil.remove(r1, b8).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b9).equals(Lists.newArrayList()));

        Assert.assertTrue(RangeUtil.remove(r1, c1).equals(Lists.newArrayList(Range.closed(2, 2), Range.closed(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, c2).equals(Lists.newArrayList(Range.closed(2, 3), Range.closed(4, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, c3).equals(Lists.newArrayList(Range.closed(2, 4), Range.closed(5, 5))));

        Assert.assertTrue(RangeUtil.remove(r1, d1).equals(Lists.newArrayList(Range.openClosed(3, 5))));
        Assert.assertTrue(
                RangeUtil.remove(r1, d2).equals(Lists.newArrayList(Range.closedOpen(2, 3), Range.openClosed(4, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, d3).equals(Lists.newArrayList(Range.closedOpen(2, 4))));

    }

    @Test
    public void testOpen() {

        Range anull = Range.closedOpen(0, 0);

        Range r1 = Range.open(2, 5);

        Range a1 = Range.open(1, 2);
        Range a2 = Range.open(1, 3);
        Range a3 = Range.open(1, 5);
        Range a4 = Range.open(1, 6);
        Range a5 = Range.open(6, 7);
        Range a6 = Range.open(5, 7);
        Range a7 = Range.open(4, 7);
        Range a8 = Range.open(2, 7);
        Range a9 = Range.open(1, 7);

        Range b1 = Range.closed(1, 2);
        Range b2 = Range.closed(1, 3);
        Range b3 = Range.closed(1, 5);
        Range b4 = Range.closed(1, 6);
        Range b5 = Range.closed(6, 7);
        Range b6 = Range.closed(5, 7);
        Range b7 = Range.closed(4, 7);
        Range b8 = Range.closed(2, 7);
        Range b9 = Range.closed(1, 7);

        Range c1 = Range.open(2, 3);
        Range c2 = Range.open(3, 4);
        Range c3 = Range.open(4, 5);

        Range d1 = Range.closed(2, 3);
        Range d2 = Range.closed(3, 4);
        Range d3 = Range.closed(4, 5);

        Assert.assertTrue(RangeUtil.remove(r1, anull).equals(Lists.newArrayList(r1)));

        Assert.assertTrue(RangeUtil.remove(r1, a1).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a2).equals(Lists.newArrayList(Range.closedOpen(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, a3).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, a4).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, a5).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a6).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, a7).equals(Lists.newArrayList(Range.openClosed(2, 4))));
        Assert.assertTrue(RangeUtil.remove(r1, a8).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, a9).equals(Lists.newArrayList()));

        Assert.assertTrue(RangeUtil.remove(r1, b1).equals(Lists.newArrayList(Range.open(2, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b2).equals(Lists.newArrayList(Range.open(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b3).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b4).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b5).equals(Lists.newArrayList(r1)));
        Assert.assertTrue(RangeUtil.remove(r1, b6).equals(Lists.newArrayList(Range.open(2, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, b7).equals(Lists.newArrayList(Range.open(2, 4))));
        Assert.assertTrue(RangeUtil.remove(r1, b8).equals(Lists.newArrayList()));
        Assert.assertTrue(RangeUtil.remove(r1, b9).equals(Lists.newArrayList()));

        Assert.assertTrue(RangeUtil.remove(r1, c1).equals(Lists.newArrayList(Range.closedOpen(3, 5))));
        Assert.assertTrue(
                RangeUtil.remove(r1, c2).equals(Lists.newArrayList(Range.openClosed(2, 3), Range.closedOpen(4, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, c3).equals(Lists.newArrayList(Range.openClosed(2, 4))));

        Assert.assertTrue(RangeUtil.remove(r1, d1).equals(Lists.newArrayList(Range.open(3, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, d2).equals(Lists.newArrayList(Range.open(2, 3), Range.open(4, 5))));
        Assert.assertTrue(RangeUtil.remove(r1, d3).equals(Lists.newArrayList(Range.open(2, 4))));

    }
}
