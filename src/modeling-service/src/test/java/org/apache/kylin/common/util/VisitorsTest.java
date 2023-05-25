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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.model.NDataModel;
import org.junit.Test;

public class VisitorsTest {

    @Test
    public void testSqlIdentifierFormatterVisitor() {
        NDataModel.NamedColumn col1 = new NDataModel.NamedColumn();
        col1.setAliasDotColumn("TBL.COL1");
        NDataModel.NamedColumn col2 = new NDataModel.NamedColumn();
        col2.setAliasDotColumn("TBL.COL1");
        NDataModel.NamedColumn tombCol = new NDataModel.NamedColumn();
        tombCol.setAliasDotColumn("TOMB");
        tombCol.setStatus(NDataModel.ColumnStatus.TOMB);

        SqlIdentifier validIdentifier = new SqlIdentifier(ImmutableList.of("col1"), SqlParserPos.ZERO);
        new SqlIdentifierFormatterVisitor("", ImmutableList.of(col1)).visit(validIdentifier);
        assertEquals("TBL.COL1", validIdentifier.toString());

        ImmutableList<NDataModel.NamedColumn> columns = ImmutableList.of(col1, col2, tombCol);
        KylinException ke = assertThrows(KylinException.class, () -> new SqlIdentifierFormatterVisitor("", columns));
        assertEquals(ServerErrorCode.DUPLICATED_COLUMN_NAME.toErrorCode().getCodeString(),
                ke.getErrorCode().getCodeString());

        NDataModel.NamedColumn col3 = new NDataModel.NamedColumn();
        col3.setAliasDotColumn("COL3");
        ke = assertThrows(KylinException.class,
                () -> new SqlIdentifierFormatterVisitor("", ImmutableList.of(col3, tombCol)));
        assertEquals(ServerErrorCode.INVALID_MODEL_TYPE.toErrorCode().getCodeString(),
                ke.getErrorCode().getCodeString());

        SqlIdentifierFormatterVisitor tombVisitor = new SqlIdentifierFormatterVisitor("", ImmutableList.of(tombCol));
        final SqlIdentifier identifier = new SqlIdentifier(ImmutableList.of("TBL", "COL4"), SqlParserPos.ZERO);
        ke = assertThrows(KylinException.class, () -> tombVisitor.visit(identifier));
        assertEquals(ServerErrorCode.COLUMN_NOT_EXIST.toErrorCode().getCodeString(), ke.getErrorCode().getCodeString());
        assertTrue(ke.getMessage().contains("unrecognized column"));

        SqlIdentifierFormatterVisitor visitor = new SqlIdentifierFormatterVisitor("", ImmutableList.of(col1));
        ke = assertThrows(KylinException.class, () -> visitor.visit(identifier));
        assertEquals(ServerErrorCode.COLUMN_NOT_EXIST.toErrorCode().getCodeString(), ke.getErrorCode().getCodeString());
        assertTrue(ke.getMessage().contains("unrecognized column"));

        final SqlIdentifier ambiguousIdentifier = new SqlIdentifier("COL3", SqlParserPos.ZERO);
        ke = assertThrows(KylinException.class, () -> visitor.visit(ambiguousIdentifier));
        assertEquals(ServerErrorCode.COLUMN_NOT_EXIST.toErrorCode().getCodeString(), ke.getErrorCode().getCodeString());
        assertTrue(ke.getMessage().contains("ambiguous column"));

        final SqlBasicCall sqlBasicCall =
                new SqlBasicCall(new SqlAsOperator(), new SqlNode[] {identifier, identifier}, SqlParserPos.ZERO);
        ke = assertThrows(KylinException.class, () -> visitor.visit(sqlBasicCall));
        assertEquals(ServerErrorCode.INVALID_PARAMETER.toErrorCode().getCodeString(),
                ke.getErrorCode().getCodeString());

        final SqlIdentifier unsupportedIdentifier =
                new SqlIdentifier(ImmutableList.of("catalog", "tbl", "col"), SqlParserPos.ZERO);
        ke = assertThrows(KylinException.class, () -> visitor.visit(unsupportedIdentifier));
        assertEquals(ServerErrorCode.COLUMN_NOT_EXIST.toErrorCode().getCodeString(),
                ke.getErrorCode().getCodeString());
    }

}
