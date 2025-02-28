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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.kylin.sdk.datasource.framework.utils.Constants;

public class ConvRownumSqlWriter extends ConvSqlWriter {

    ConvRownumSqlWriter(SqlConverter.IConfigurer configurer) throws SQLException {
        super(configurer);
    }

    private boolean printSelectForRownumInWithCLause = false;
    private static final String ALIAS_ROWNUM = Constants.ALIAS_ROWNUM_ORCALE;
    private static final String SQL_ROWSTART = "SELECT * \nFROM(\n\tSELECT T.*, ROWNUM " + ALIAS_ROWNUM
            + "\n\tFROM ( \n\t";
    private static final String SQL_ROWEND_NOLIMIT = "\n\t) T\n) ";
    private static final String SQL_ROWEND_LIMIT_INNER = "\n\t) T WHERE ROWNUM <= ";
    private static final String SQL_ROWEND_LIMIT_OUTER = " \n) \nWHERE 1 = 1 AND ";
    private Deque<String> lastAliasRownumNameStack = new ArrayDeque();
    private int rownumCounter = 1;

    @Override
    protected SqlWriter.Frame startList(SqlWriter.FrameType frameType, String keyword, String open, String close) {
        /*
        For Oracle <= 11g, paging sql template:
        
        SELECT *
        FROM (
            SELECT T.*, ROWNUM ROWNUM__
            FROM (
            [origin sql]
            ) T WHERE ROWNUM <= LIMIT + OFFSET
        )
        WHERE 1 = 1 AND ROWNUM__ BETWEEN OFFSET + 1 AND LIMIT + OFFSET
        
        or
        
        SELECT * FROM
        (
            SELECT T.*, ROWNUM ROWNUM__
            FROM (
            [origin sql]
            ) T WHERE ROWNUM <= LIMIT
        )
        WHERE 1 = 1 AND ROWNUM__ <= LIMIT
        
        */
        if (this.frame != null && this.frame.frameType == SqlWriter.FrameTypeEnum.ORDER_BY
                && (frameType == SqlWriter.FrameTypeEnum.SELECT || frameType == SqlWriter.FrameTypeEnum.SETOP
                        || frameType == SqlWriter.FrameTypeEnum.SIMPLE)) {
            this.keyword(masageSqlRowStart());
        }
        return super.startList(frameType, keyword, open, close);
    }

    @Override
    public void fetchOffset(SqlNode fetch, SqlNode offset) {
        doWriteRowNum(fetch, offset);
    }

    @Override
    protected void doWriteRowNum(SqlNode fetch, SqlNode offset) {
        if (this.frame != null && this.frame.frameType == SqlWriter.FrameTypeEnum.ORDER_BY) {
            final SqlWriter.Frame fetchFrame = this.startList(SqlWriter.FrameTypeEnum.FETCH);
            this.newlineAndIndent();
            if (fetch != null) {
                this.keyword(SQL_ROWEND_LIMIT_INNER);
                fetch.unparse(this, -1, -1);
                if (offset != null) {
                    this.keyword(" + ");
                    offset.unparse(this, -1, -1);
                }
                this.keyword(SQL_ROWEND_LIMIT_OUTER);
                String lastAliasRownumName = lastAliasRownumNameStack.isEmpty() ? "ROWNUM"
                        : lastAliasRownumNameStack.pop();
                this.keyword(lastAliasRownumName);
                if (offset != null) {
                    this.keyword(" BETWEEN ");
                    offset.unparse(this, -1, -1);
                    this.keyword(" + 1 AND ");
                    offset.unparse(this, -1, -1);
                    this.keyword(" + ");
                    fetch.unparse(this, -1, -1);
                } else {
                    this.keyword(" <= ");
                    fetch.unparse(this, -1, -1);
                }
            } else {
                if (!lastAliasRownumNameStack.isEmpty()) {
                    lastAliasRownumNameStack.pop();
                }
                this.keyword(SQL_ROWEND_NOLIMIT);
            }
            this.endList(fetchFrame);
        }
    }

    @Override
    public void writeWith(SqlCall call, int leftPrec, int rightPrec) {
        /*
        For Oracle <= 11g, to add fetch rows should be:
        origin sql => SELECT * FROM  ([origin sql]) WHERE ROWNUM <= [FETCH_SIZE]
        Here we should print "SELECT * FROM (" before print origin sql
        */
        printSelectForRownumInWithCLause = (this.frame != null
                && this.frame.frameType == SqlWriter.FrameTypeEnum.ORDER_BY);
        super.writeWith(call, leftPrec, rightPrec);
    }

    @Override
    protected void outputBetweenWithListAndWithbody() {
        if (printSelectForRownumInWithCLause) {
            this.keyword(masageSqlRowStart());
        }
    }

    private String masageSqlRowStart() {
        String lastAliasRownumName = ALIAS_ROWNUM + rownumCounter;
        rownumCounter++;
        lastAliasRownumNameStack.push(lastAliasRownumName);
        return SQL_ROWSTART.replace(ALIAS_ROWNUM, lastAliasRownumName);
    }
}
