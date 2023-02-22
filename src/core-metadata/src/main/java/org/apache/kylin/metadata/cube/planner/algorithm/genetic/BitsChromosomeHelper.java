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

package org.apache.kylin.metadata.cube.planner.algorithm.genetic;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kylin.metadata.cube.planner.algorithm.LayoutStats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class BitsChromosomeHelper {

    public final double spaceLimit;
    private final LayoutStats layoutStats;
    private final LayoutEncoder layoutEncoder;

    public BitsChromosomeHelper(final double spaceLimit, final LayoutStats layoutStats) {
        this.spaceLimit = spaceLimit;
        this.layoutStats = layoutStats;
        this.layoutEncoder = new LayoutEncoder(layoutStats.getAllLayoutsForSelection());
    }

    public ImmutableSet<BigInteger> getMandatoryLayouts() {
        return layoutStats.getAllLayoutsForMandatory();
    }

    public List<BigInteger> toLayoutList(BitSet bits) {
        return layoutEncoder.toLayoutList(bits);
    }

    public double getLayoutSize(Set<BigInteger> layouts) {
        double ret = 0;
        for (BigInteger layout : layouts) {
            ret += layoutStats.getLayoutSize(layout);
        }
        return ret;
    }

    public double getLayoutSizeByBitIndex(int index) {
        return layoutStats.getLayoutSize(layoutEncoder.layoutDomain.get(index));
    }

    public int getLength() {
        return layoutEncoder.layoutDomain.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BitsChromosomeHelper that = (BitsChromosomeHelper) o;
        return Objects.equals(layoutEncoder, that.layoutEncoder);

    }

    @Override
    public int hashCode() {
        return layoutEncoder != null ? layoutEncoder.hashCode() : 0;
    }

    private static class LayoutEncoder {
        public final ImmutableList<BigInteger> layoutDomain;

        public LayoutEncoder(Set<BigInteger> layoutSet) {
            List<BigInteger> layoutList = Lists.newArrayList(layoutSet);
            layoutList.sort(Collections.reverseOrder());
            this.layoutDomain = ImmutableList.copyOf(layoutList);
        }

        public List<BigInteger> toLayoutList(BitSet bits) {
            List<BigInteger> layouts = Lists.newArrayListWithExpectedSize(bits.cardinality());
            for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
                layouts.add(layoutDomain.get(i));
            }
            return layouts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            LayoutEncoder that = (LayoutEncoder) o;
            return Objects.equals(layoutDomain, that.layoutDomain);

        }

        @Override
        public int hashCode() {
            return layoutDomain != null ? layoutDomain.hashCode() : 0;
        }
    }
}
