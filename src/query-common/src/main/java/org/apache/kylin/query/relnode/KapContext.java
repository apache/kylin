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

package org.apache.kylin.query.relnode;

import java.util.Collection;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public class KapContext {
    static final ThreadLocal<KapRel> _inputRel = new ThreadLocal<>();
    static final ThreadLocal<RelDataType> _resultType = new ThreadLocal<>();
    private KapContext() {
    }

    public static KapRel getKapRel() {
        return _inputRel.get();
    }

    public static void setKapRel(KapRel kapRel) {
        _inputRel.set(kapRel);
    }

    public static RelDataType getRowType() {
        return _resultType.get();
    }

    public static void setRowType(RelDataType relDataType) {
        _resultType.set(relDataType);
    }

    public static void clean() {
        _inputRel.set(null);
        _resultType.remove();
    }

    public static void amendAllColsIfNoAgg(RelNode kapRel) {
        if (kapRel == null || ((KapRel) kapRel).getContext() == null || kapRel instanceof KapTableScan)
            return;

        OLAPContext context = ((KapRel) kapRel).getContext();
        // add columns of context's TopNode to context when there are no agg rel
        if (kapRel instanceof KapProjectRel && !((KapProjectRel) kapRel).isMerelyPermutation()) {
            ((KapRel) kapRel).getColumnRowType().getSourceColumns().stream().flatMap(Collection::stream)
                    .filter(context::isOriginAndBelongToCtxTables).forEach(context.allColumns::add);
        } else if (kapRel instanceof KapValuesRel) {
            ((KapRel) kapRel).getColumnRowType().getAllColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.allColumns::add);
        } else if (kapRel instanceof KapWindowRel) {
            ((KapWindowRel) kapRel).getGroupingColumns().stream().filter(context::isOriginAndBelongToCtxTables)
                    .forEach(context.allColumns::add);
        } else if (kapRel instanceof KapJoinRel) {
            amendAllColsIfNoAgg(kapRel.getInput(0));
            amendAllColsIfNoAgg(kapRel.getInput(1));
        } else {
            amendAllColsIfNoAgg(kapRel.getInput(0));
        }
    }
}
