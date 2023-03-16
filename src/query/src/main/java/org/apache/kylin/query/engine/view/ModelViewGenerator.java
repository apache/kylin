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

package org.apache.kylin.query.engine.view;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class ModelViewGenerator {

    private final NDataModel model;

    public ModelViewGenerator(NDataModel model) {
        this.model = model;
    }

    public String generateViewSQL() {
        return getFlatTableSQL();
    }

    private String getFlatTableSQL() {

        // select columns
        StringBuilder builder = new StringBuilder("SELECT ");
        Iterator<TblColRef> cols = listModelViewColumns().iterator();
        if (!cols.hasNext()) {
            // in case no dims at all (which won't happen normally)
            // return all cols with *
            builder.append(" * ");
        } else {
            while (cols.hasNext()) {
                TblColRef colRef = cols.next();
                builder.append(quote(colRef, getColumnNameFromModel(colRef)));
                if (cols.hasNext()) {
                    builder.append(',');
                }
            }
        }

        // joins
        // fact table
        builder.append(" FROM ").append(quote(model.getRootFactTable()));
        for (JoinTableDesc joinTable : model.getJoinTables()) {
            JoinDesc join = joinTable.getJoin();
            // join
            builder.append(" ").append(join.getType()).append(" JOIN ");
            builder.append(quote(joinTable.getTableRef()));
            // condition
            if (join.getNonEquiJoinCondition() != null) {
                builder.append(" ON ").append(join.getNonEquiJoinCondition().getExpr()); // non-equi join expr is quoted already
            } else {
                builder.append(" ON ");
                for (int i = 0; i < join.getPrimaryKeyColumns().length; i++) {
                    builder.append(quote(join.getPrimaryKeyColumns()[i])).append(" = ")
                            .append(quote(join.getForeignKeyColumns()[i]));
                    if (i != join.getPrimaryKeyColumns().length - 1) {
                        builder.append(" AND ");
                    }
                }
            }
        }

        return builder.toString();
    }

    private String quote(String... ids) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ids.length; i++) {
            sb.append(Quoting.DOUBLE_QUOTE.string).append(ids[i]).append(Quoting.DOUBLE_QUOTE.string);
            if (i != ids.length - 1) {
                sb.append('.');
            }
        }
        return sb.toString();
    }

    private String quote(TableRef ref) {
        if (ref.getTableDesc().getCaseSensitiveDatabase().equals("null")) {
            return quote(ref.getTableDesc().getCaseSensitiveName()) + " AS " + quote(ref.getAlias());
        } else {
            return quote(ref.getTableDesc().getCaseSensitiveDatabase(), ref.getTableDesc().getCaseSensitiveName())
                    + " AS " + quote(ref.getAlias());
        }
    }

    private String quote(TblColRef tblColRef) {
        return quote(tblColRef.getTableAlias(), tblColRef.getName());
    }

    private String quote(TblColRef tblColRef, String alias) {
        return quote(tblColRef.getTableAlias(), tblColRef.getName()) + " AS " + quote(alias);
    }

    private Set<TblColRef> listModelViewColumns() {
        Set<TblColRef> colRefs = new HashSet<>();

        // add all dims
        model.getEffectiveDimensions().forEach((id, colRef) -> colRefs.add(colRef));

        // add all measure source columns
        model.getEffectiveMeasures().forEach((id, measure) -> colRefs.addAll(measure.getFunction().getColRefs()));

        // add all cc source columns
        List<TblColRef> ccCols = colRefs.stream().filter(col -> col.getColumnDesc().isComputedColumn())
                .collect(Collectors.toList());
        colRefs.addAll(getComputedColumnSourceColumns(ccCols));

        return colRefs;
    }

    /**
     * parse cc expr and find all table columns ref
     * @param ccCols
     * @return
     */
    private Set<TblColRef> getComputedColumnSourceColumns(List<TblColRef> ccCols) {
        List<String> ccExprs = ccCols.stream().map(colRef -> model.findCCByCCColumnName(colRef.getName()))
                .filter(Objects::nonNull).map(ComputedColumnDesc::getExpression).collect(Collectors.toList());

        try {
            SqlSelect select = (SqlSelect) CalciteParser.parse("select " + String.join(",", ccExprs),
                    this.model != null ? this.model.getProject() : null);

            return getAllIdentifiers(select).stream().map(SqlIdentifier::toString).map(model::getColRef)
                    .collect(Collectors.toSet());
        } catch (SqlParseException e) {
            return new HashSet<>();
        }
    }

    private String getColumnNameFromModel(TblColRef colRef) {
        Integer id = model.getEffectiveCols().inverse().get(colRef);
        return id == null ? null : model.getNameByColumnId(id).toUpperCase(Locale.getDefault());
    }

    private static List<SqlIdentifier> getAllIdentifiers(SqlNode sqlNode) {
        if (sqlNode instanceof SqlNodeList) {
            return getAllIdentifiersFromList(((SqlNodeList) sqlNode).getList());
        } else if (sqlNode instanceof SqlCall) {
            return getAllIdentifiersFromList(((SqlCall) sqlNode).getOperandList());
        } else if (sqlNode instanceof SqlIdentifier) {
            return Lists.newArrayList((SqlIdentifier) sqlNode);
        } else {
            return Lists.newArrayList();
        }
    }

    private static List<SqlIdentifier> getAllIdentifiersFromList(List<SqlNode> nodes) {
        return nodes.stream().map(ModelViewGenerator::getAllIdentifiers).flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
