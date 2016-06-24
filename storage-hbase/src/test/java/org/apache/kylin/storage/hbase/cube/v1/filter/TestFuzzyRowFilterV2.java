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

package org.apache.kylin.storage.hbase.cube.v1.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestFuzzyRowFilterV2 {
    @Test
    public void testSatisfiesNoUnsafeForward() {

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, (byte) -128, 1, 0, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, (byte) -128, 2, 0, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, 2, 1, 3, 3 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS,
                FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, 1, 1, 3, 0 }, // row to check
                        0, 5, new byte[] { 1, 2, 0, 3 }, // fuzzy row
                        new byte[] { 0, 0, 1, 0 })); // mask

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, 1, 1, 3, 0 }, 0, 5, new byte[] { 1, (byte) 245, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(false, new byte[] { 1, 2, 1, 0, 1 }, 0, 5, new byte[] { 0, 1, 2 }, new byte[] { 1, 0, 0 }));
    }

    @Test
    public void testSatisfiesForward() {

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfies(false, new byte[] { 1, (byte) -128, 1, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(false, new byte[] { 1, (byte) -128, 2, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfies(false, new byte[] { 1, 2, 1, 3, 3 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS,
                FuzzyRowFilterV2.satisfies(false, new byte[] { 1, 1, 1, 3, 0 }, // row to check
                        new byte[] { 1, 2, 0, 3 }, // fuzzy row
                        new byte[] { -1, -1, 0, -1 })); // mask

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(false, new byte[] { 1, 1, 1, 3, 0 }, new byte[] { 1, (byte) 245, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(false, new byte[] { 1, 2, 1, 0, 1 }, new byte[] { 0, 1, 2 }, new byte[] { 0, -1, -1 }));
    }

    @Test
    public void testSatisfiesReverse() {
        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, (byte) -128, 1, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, (byte) -128, 2, 0, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 2, 3, 1, 1, 1 }, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, 2, 1, 3, 3 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, (byte) 245, 1, 3, 0 }, new byte[] { 1, 1, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, 3, 1, 3, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 2, 1, 1, 1, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfies(true, new byte[] { 1, 2, 1, 0, 1 }, new byte[] { 0, 1, 2 }, new byte[] { 0, -1, -1 }));
    }

    @Test
    public void testSatisfiesNoUnsafeReverse() {
        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, (byte) -128, 1, 0, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, (byte) -128, 2, 0, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 2, 3, 1, 1, 1 }, 0, 5, new byte[] { 1, 0, 1 }, new byte[] { 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.YES, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, 2, 1, 3, 3 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, (byte) 245, 1, 3, 0 }, 0, 5, new byte[] { 1, 1, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, 3, 1, 3, 0 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 2, 1, 1, 1, 0 }, 0, 5, new byte[] { 1, 2, 0, 3 }, new byte[] { 0, 0, 1, 0 }));

        Assert.assertEquals(FuzzyRowFilterV2.SatisfiesCode.NEXT_EXISTS, FuzzyRowFilterV2.satisfiesNoUnsafe(true, new byte[] { 1, 2, 1, 0, 1 }, 0, 5, new byte[] { 0, 1, 2 }, new byte[] { 1, 0, 0 }));
    }

    @Test
    public void testGetNextForFuzzyRuleForward() {
        assertNext(false, new byte[] { 0, 1, 2 }, // fuzzy row
                new byte[] { 0, -1, -1 }, // mask
                new byte[] { 1, 2, 1, 0, 1 }, // current
                new byte[] { 2, 1, 2, 0, 0 }); // expected next

        assertNext(false, new byte[] { 0, 1, 2 }, // fuzzy row
                new byte[] { 0, -1, -1 }, // mask
                new byte[] { 1, 1, 2, 0, 1 }, // current
                new byte[] { 1, 1, 2, 0, 2 }); // expected next

        assertNext(false, new byte[] { 0, 1, 0, 2, 0 }, // fuzzy row
                new byte[] { 0, -1, 0, -1, 0 }, // mask
                new byte[] { 1, 0, 2, 0, 1 }, // current
                new byte[] { 1, 1, 0, 2, 0 }); // expected next

        assertNext(false, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }, new byte[] { 1, (byte) 128, 2, 0, 1 }, new byte[] { 1, (byte) 129, 1, 0, 0 });

        assertNext(false, new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 }, new byte[] { 5, 1, 0, 1 }, new byte[] { 5, 1, 1, 1 });

        assertNext(false, new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 }, new byte[] { 5, 1, 0, 1, 1 }, new byte[] { 5, 1, 0, 1, 2 });

        assertNext(false, new byte[] { 0, 1, 0, 0 }, // fuzzy row
                new byte[] { 0, -1, 0, 0 }, // mask
                new byte[] { 5, 1, (byte) 255, 1 }, // current
                new byte[] { 5, 1, (byte) 255, 2 }); // expected next

        assertNext(false, new byte[] { 0, 1, 0, 1 }, // fuzzy row
                new byte[] { 0, -1, 0, -1 }, // mask
                new byte[] { 5, 1, (byte) 255, 1 }, // current
                new byte[] { 6, 1, 0, 1 }); // expected next

        assertNext(false, new byte[] { 0, 1, 0, 1 }, // fuzzy row
                new byte[] { 0, -1, 0, -1 }, // mask
                new byte[] { 5, 1, (byte) 255, 0 }, // current
                new byte[] { 5, 1, (byte) 255, 1 }); // expected next

        assertNext(false, new byte[] { 5, 1, 1, 0 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 5, 1, (byte) 255, 1 }, new byte[] { 5, 1, (byte) 255, 2 });

        assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 1, 1, 2, 2 }, new byte[] { 1, 1, 2, 3 });

        assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 1, 1, 3, 2 }, new byte[] { 1, 1, 3, 3 });

        assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 1, 2, 3 }, new byte[] { 1, 1, 2, 4 });

        assertNext(false, new byte[] { 1, 1, 1, 1 }, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 1, 3, 2 }, new byte[] { 1, 1, 3, 3 });

        assertNext(false, new byte[] { 1, 1, 0, 0 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 0, 1, 3, 2 }, new byte[] { 1, 1, 0, 0 });

        // No next for this one
        Assert.assertNull(FuzzyRowFilterV2.getNextForFuzzyRule(new byte[] { 2, 3, 1, 1, 1 }, // row to check
                new byte[] { 1, 0, 1 }, // fuzzy row
                new byte[] { -1, 0, -1 })); // mask
        Assert.assertNull(FuzzyRowFilterV2.getNextForFuzzyRule(new byte[] { 1, (byte) 245, 1, 3, 0 }, new byte[] { 1, 1, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
        Assert.assertNull(FuzzyRowFilterV2.getNextForFuzzyRule(new byte[] { 1, 3, 1, 3, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
        Assert.assertNull(FuzzyRowFilterV2.getNextForFuzzyRule(new byte[] { 2, 1, 1, 1, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
    }

    @Test
    public void testGetNextForFuzzyRuleReverse() {
        assertNext(true, new byte[] { 0, 1, 2 }, // fuzzy row
                new byte[] { 0, -1, -1 }, // mask
                new byte[] { 1, 2, 1, 0, 1 }, // current
                // TODO: should be {1, 1, 3} ?
                new byte[] { 1, 1, 2, (byte) 0xFF, (byte) 0xFF }); // expected next

        assertNext(true, new byte[] { 0, 1, 0, 2, 0 }, // fuzzy row
                new byte[] { 0, -1, 0, -1, 0 }, // mask
                new byte[] { 1, 2, 1, 3, 1 }, // current
                // TODO: should be {1, 1, 1, 3} ?
                new byte[] { 1, 1, 0, 2, 0 }); // expected next

        assertNext(true, new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }, new byte[] { 1, (byte) 128, 2, 0, 1 },
                // TODO: should be {1, (byte) 128, 2} ?
                new byte[] { 1, (byte) 128, 1, (byte) 0xFF, (byte) 0xFF });

        assertNext(true, new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 }, new byte[] { 5, 1, 0, 2, 1 },
                // TODO: should be {5, 1, 0, 2} ?
                new byte[] { 5, 1, 0, 1, (byte) 0xFF });

        assertNext(true, new byte[] { 0, 1, 0, 0 }, // fuzzy row
                new byte[] { 0, -1, 0, 0 }, // mask
                new byte[] { 5, 1, (byte) 255, 1 }, // current
                new byte[] { 5, 1, (byte) 255, 0 }); // expected next

        assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
                new byte[] { 0, -1, 0, -1 }, // mask
                new byte[] { 5, 1, 0, 1 }, // current
                new byte[] { 4, 1, (byte) 255, 1 }); // expected next

        assertNext(true, new byte[] { 0, 1, 0, 1 }, // fuzzy row
                new byte[] { 0, -1, 0, -1 }, // mask
                new byte[] { 5, 1, (byte) 255, 0 }, // current
                new byte[] { 5, 1, (byte) 254, 1 }); // expected next

        assertNext(true, new byte[] { 1, 1, 0, 0 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 2, 1, 3, 2 },
                // TODO: should be {1, 0} ?
                new byte[] { 1, 1, 0, 0 });

        assertNext(true, new byte[] { 1, 0, 1 }, // fuzzy row
                new byte[] { -1, 0, -1 }, // mask
                new byte[] { 2, 3, 1, 1, 1 }, // row to check
                // TODO: should be {1, (byte) 0xFF, 2} ?
                new byte[] { 1, 0, 1, (byte) 0xFF, (byte) 0xFF });

        assertNext(true, new byte[] { 1, 1, 0, 3 }, new byte[] { -1, -1, 0, -1 }, new byte[] { 1, (byte) 245, 1, 3, 0 },
                // TODO: should be {1, 1, (byte) 255, 4} ?
                new byte[] { 1, 1, 0, 3, (byte) 0xFF });

        assertNext(true, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }, new byte[] { 1, 3, 1, 3, 0 },
                // TODO: should be 1, 2, (byte) 255, 4 ?
                new byte[] { 1, 2, 0, 3, (byte) 0xFF });

        assertNext(true, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }, new byte[] { 2, 1, 1, 1, 0 },
                // TODO: should be {1, 2, (byte) 255, 4} ?
                new byte[] { 1, 2, 0, 3, (byte) 0xFF });

        assertNext(true,
                // TODO: should be null?
                new byte[] { 1, 0, 1 }, new byte[] { -1, 0, -1 }, new byte[] { 1, (byte) 128, 2 }, new byte[] { 1, (byte) 128, 1 });

        assertNext(true,
                // TODO: should be null?
                new byte[] { 0, 1, 0, 1 }, new byte[] { 0, -1, 0, -1 }, new byte[] { 5, 1, 0, 2 }, new byte[] { 5, 1, 0, 1 });

        assertNext(true,
                // TODO: should be null?
                new byte[] { 5, 1, 1, 0 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 5, 1, (byte) 0xFF, 1 }, new byte[] { 5, 1, (byte) 0xFF, 0 });

        assertNext(true,
                // TODO: should be null?
                new byte[] { 1, 1, 1, 1 }, new byte[] { -1, -1, 0, 0 }, new byte[] { 1, 1, 2, 2 }, new byte[] { 1, 1, 2, 1 });

        assertNext(true,
                // TODO: should be null?
                new byte[] { 1, 1, 1, 1 }, new byte[] { 0, 0, 0, 0 }, new byte[] { 1, 1, 2, 3 }, new byte[] { 1, 1, 2, 2 });

        Assert.assertNull(FuzzyRowFilterV2.getNextForFuzzyRule(true, new byte[] { 1, 1, 1, 3, 0 }, new byte[] { 1, 2, 0, 3 }, new byte[] { -1, -1, 0, -1 }));
    }

    private static void assertNext(boolean reverse, byte[] fuzzyRow, byte[] mask, byte[] current, byte[] expected) {
        KeyValue kv = KeyValue.createFirstOnRow(current);
        byte[] nextForFuzzyRule = FuzzyRowFilterV2.getNextForFuzzyRule(reverse, kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(), fuzzyRow, mask);
        Assert.assertEquals(Bytes.toStringBinary(expected), Bytes.toStringBinary(nextForFuzzyRule));
    }
}
