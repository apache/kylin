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
package org.apache.kylin.query.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.kylin.query.engine.KECalciteConfig;

public class KylinSqlValidator extends SqlValidatorImpl {

    public KylinSqlValidator(SqlValidatorImpl calciteSqlValidator) {
        super(calciteSqlValidator.getOperatorTable(), calciteSqlValidator.getCatalogReader(),
                calciteSqlValidator.getTypeFactory(), calciteSqlValidator.getConformance());
    }

    @Override
    protected boolean addOrExpandField(List<SqlNode> selectItems, Set<String> aliases,
            List<Map.Entry<String, RelDataType>> types, boolean includeSystemVars, SelectScope scope, SqlIdentifier id,
            RelDataTypeField field) {
        if (isInternalFiled(field)) {
            return false;
        }
        return super.addOrExpandField(selectItems, aliases, types, includeSystemVars, scope, id, field);
    }

    private boolean isInternalFiled(RelDataTypeField field) {
        if ((field instanceof KylinRelDataTypeFieldImpl)
                && KylinRelDataTypeFieldImpl.ColumnType.CC_FIELD == ((KylinRelDataTypeFieldImpl) field)
                        .getColumnType()) {
            // if exposeComputedColumn=true, regard CC columns as non-internal field
            return !KECalciteConfig.current().exposeComputedColumn();
        }
        return field.getName().startsWith("_KY_");
    }

    @Override
    protected RelDataType getLogicalSourceRowType(RelDataType sourceRowType, SqlInsert insert) {
        final RelDataType superType = super.getLogicalSourceRowType(sourceRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }

    @Override
    protected RelDataType getLogicalTargetRowType(RelDataType targetRowType, SqlInsert insert) {
        final RelDataType superType = super.getLogicalTargetRowType(targetRowType, insert);
        return ((JavaTypeFactory) typeFactory).toSql(superType);
    }
}
