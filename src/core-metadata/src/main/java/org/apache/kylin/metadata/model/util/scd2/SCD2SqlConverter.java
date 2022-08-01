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
package org.apache.kylin.metadata.model.util.scd2;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.TableRef;

/**
 * convert simplified cond to sql
 */
public class SCD2SqlConverter {

    public static final SCD2SqlConverter INSTANCE = new SCD2SqlConverter();

    /**
     * SimplifiedNonEquiJoinCondition must not be empty
     *
     * you should check scd2 expression before call this method
     * @param joinDesc
     * @return
     */
    public String genSCD2SqlStr(JoinDesc joinDesc,
            List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> simplifiedNonEquiJoinConditions) {
        StringBuilder sb = new StringBuilder();

        sb.append("select * from ").append(toJoinDescQuotedString(joinDesc))
                .append(" " + SqlKind.AND.sql + " " + genNonEquiWithSimplified(simplifiedNonEquiJoinConditions));

        return sb.toString();
    }

    private String quotedIdentifierStr(String identifier) {
        return Quoting.DOUBLE_QUOTE.string + identifier + Quoting.DOUBLE_QUOTE.string;
    }

    private String quotedTableRefStr(TableRef tableRef) {
        return quotedIdentifierStr(tableRef.getTableDesc().getDatabase()) + "."
                + quotedIdentifierStr(tableRef.getTableDesc().getName());
    }

    private String quotedColumnStr(String colStr) {
        String[] cols = StringSplitter.split(colStr, ".");
        return quotedIdentifierStr(cols[0]) + "." + quotedIdentifierStr(cols[1]);
    }

    private String genNonEquiWithSimplified(List<NonEquiJoinCondition.SimplifiedNonEquiJoinCondition> simplified) {

        return simplified.stream()
                .map(simplifiedNonEquiJoinCondition -> "("
                        + quotedColumnStr(simplifiedNonEquiJoinCondition.getForeignKey())
                        + simplifiedNonEquiJoinCondition.getOp().sql
                        + quotedColumnStr(simplifiedNonEquiJoinCondition.getPrimaryKey()) + ")")
                .collect(Collectors.joining(" " + SqlKind.AND.sql + " "));

    }

    private String toJoinDescQuotedString(JoinDesc join) {

        StringBuilder result = new StringBuilder();
        result.append(" ").append(quotedTableRefStr(join.getFKSide())).append(" AS ")
                .append(quotedIdentifierStr(join.getFKSide().getAlias())).append(" ").append(join.getType())
                .append(" JOIN ").append(quotedTableRefStr(join.getPKSide())).append(" AS ")
                .append(quotedIdentifierStr(join.getPKSide().getAlias())).append(" ON ");
        for (int i = 0; i < join.getForeignKey().length; i++) {
            String fk = quotedColumnStr(join.getForeignKey()[i]);
            String pk = quotedColumnStr(join.getPrimaryKey()[i]);
            if (i > 0) {
                result.append(" " + SqlKind.AND.sql + " ");
            }
            result.append(fk).append("=").append(pk);
        }
        return result.toString();
    }
}
