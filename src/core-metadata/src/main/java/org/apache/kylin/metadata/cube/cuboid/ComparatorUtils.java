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

package org.apache.kylin.metadata.cube.cuboid;

import java.util.Comparator;

import org.apache.kylin.metadata.model.TableExtDesc.ColumnStats;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TblColRef.FilterColEnum;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Ordering;

/**
 * Used for select the best-cost candidate for query or auto-modeling
 */
public class ComparatorUtils {

    private ComparatorUtils() {
    }

    public static Comparator<NLayoutCandidate> simple() {
        return (o1, o2) -> o2.getLayoutEntity().getOrderedDimensions().size()
                - o1.getLayoutEntity().getOrderedDimensions().size();
    }

    /**
     * Return comparator for non-filter column
     */
    public static Comparator<TblColRef> nonFilterColComparator() {
        return (col1, col2) -> {
            Preconditions.checkArgument(col1 != null && col1.getFilterLevel() == FilterColEnum.NONE);
            Preconditions.checkArgument(col2 != null && col2.getFilterLevel() == FilterColEnum.NONE);
            return col1.getIdentity().compareToIgnoreCase(col2.getIdentity());
        };
    }

    /**
     * Return comparator for filter column
     */
    public static Comparator<TblColRef> filterColComparator(ChooserContext chooserContext) {
        return Ordering.from(filterLevelComparator()).compound(cardinalityComparator(chooserContext));
    }

    /**
     * cannot deal with null col, if need compare null cols, plz add another comparator,
     * for example, @see nullLastComparator
     *
     * @return
     */
    private static Comparator<TblColRef> filterLevelComparator() {
        return (col1, col2) -> {
            // priority desc
            if (col1 != null && col2 != null) {
                return col2.getFilterLevel().getPriority() - col1.getFilterLevel().getPriority();
            }
            return 0;
        };
    }

    public static Comparator<TblColRef> cardinalityComparator(ChooserContext chooserContext) {
        return (col1, col2) -> {
            if (col1 == null || col2 == null)
                return 0;

            final ColumnStats ret1 = chooserContext.getColumnStats(col1);
            final ColumnStats ret2 = chooserContext.getColumnStats(col2);
            //null last
            if (ret2 == null && ret1 == null) {
                return col1.getIdentity().compareToIgnoreCase(col2.getIdentity());
            } else if (ret2 == null) {
                return -1;
            } else if (ret1 == null) {
                return 1;
            }
            // getCardinality desc
            return Long.compare(ret2.getCardinality(), ret1.getCardinality());
        };
    }

    public static <T> Comparator<T> nullLastComparator() {
        return (t1, t2) -> {
            if (t1 == null && t2 != null) {
                return 1;
            } else if (t2 == null && t1 != null) {
                return -1;
            }
            return 0;
        };
    }
}
