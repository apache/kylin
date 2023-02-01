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

package org.apache.kylin.metadata.model.graph;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.ColumnDesc;

import lombok.NonNull;

public class DefaultJoinEdgeMatcher implements IJoinEdgeMatcher {

    @Override
    public boolean matches(@NonNull Edge join1, @NonNull Edge join2) {
        if (join1.isLeftJoin() != join2.isLeftJoin() && !join1.isLeftOrInnerJoin() && !join2.isLeftOrInnerJoin()) {
            return false;
        }

        if (!Objects.equals(join1.nonEquiJoinCondition, join2.nonEquiJoinCondition)) {
            return false;
        }

        if (join1.isLeftJoin()) {
            return columnDescEquals(join1.leftCols, join2.leftCols)
                    && columnDescEquals(join1.rightCols, join2.rightCols);
        } else {
            return (columnDescEquals(join1.leftCols, join2.leftCols)
                    && columnDescEquals(join1.rightCols, join2.rightCols))
                    || (columnDescEquals(join1.leftCols, join2.rightCols)
                            && columnDescEquals(join1.rightCols, join2.leftCols));
        }
    }

    private boolean columnDescEquals(ColumnDesc[] a, ColumnDesc[] b) {
        if (a.length != b.length) {
            return false;
        }

        List<ColumnDesc> oneList = Lists.newArrayList(a);
        List<ColumnDesc> anotherList = Lists.newArrayList(b);

        for (ColumnDesc obj : oneList) {
            anotherList.removeIf(dual -> columnDescEquals(obj, dual));
        }
        return anotherList.isEmpty();
    }

    protected boolean columnDescEquals(ColumnDesc a, ColumnDesc b) {
        return Objects.equals(a, b);
    }
}
