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

package org.apache.kylin.query.util;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.Getter;

public class RawSql {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final Pattern WHITE_SPACE_PATTERN = Pattern.compile("\\s");
    public static final String SELECT = "select";
    public static final String WITH = "with";
    public static final String EXPLAIN = "explain";

    // User original sql
    @Getter
    private final String sql;
    // Statement block list
    private final List<RawSqlBlock> stmtBlockList;
    // All block list including statement block & comment block
    private final List<RawSqlBlock> allBlockList;
    // Cache
    private String statementStringCache;
    private String fullTextStringCache;

    public RawSql(String sql, List<RawSqlBlock> stmtBlockList, List<RawSqlBlock> allBlockList) {
        this.sql = sql.trim();
        this.stmtBlockList = stmtBlockList;
        this.allBlockList = allBlockList;
        removeStatementEndedSemicolon();
    }

    public String getStatementString() {
        if (statementStringCache != null) {
            return statementStringCache;
        }
        StringBuilder stmt = new StringBuilder();
        int prevEndLine = -1;
        for (RawSqlBlock block : stmtBlockList) {
            if (block.getBeginLine() > prevEndLine) {
                if (prevEndLine != -1) {
                    stmt.append(LINE_SEPARATOR);
                }
                stmt.append(block.getTrimmedText());
            } else if (block.getBeginLine() == prevEndLine) {
                stmt.append(" ");
                stmt.append(block.getTrimmedText());
            }
            prevEndLine = block.getEndLine();
        }
        statementStringCache = stmt.toString();
        return statementStringCache;
    }

    public String getStatementStringWithoutHints() {
        StringBuilder stmt = new StringBuilder();
        int prevEndLine = -1;
        for (RawSqlBlock block : stmtBlockList) {
            if (RawSqlBlock.Type.STATEMENT == block.getType()) {
                if (block.getBeginLine() > prevEndLine) {
                    if (prevEndLine != -1) {
                        stmt.append(LINE_SEPARATOR);
                    }
                    stmt.append(block.getTrimmedText());
                } else if (block.getBeginLine() == prevEndLine) {
                    stmt.append(" ");
                    stmt.append(block.getTrimmedText());
                }
                prevEndLine = block.getEndLine();
            }
        }
        return stmt.toString();
    }

    public String getFullTextString() {
        if (fullTextStringCache != null) {
            return fullTextStringCache;
        }
        StringBuilder fullText = new StringBuilder();
        for (RawSqlBlock block : allBlockList) {
            fullText.append(block.getText());
        }
        fullTextStringCache = fullText.toString();
        return fullTextStringCache;
    }

    public String getFirstHintString() {
        RawSqlBlock firstHintBlock = stmtBlockList.stream()
                .filter(stmtBlock -> RawSqlBlock.Type.HINT == stmtBlock.getType())
                .findFirst()
                .orElse(null);
        return firstHintBlock == null ? null : firstHintBlock.getText();
    }

    public void autoAppendLimit(KylinConfig kylinConfig, int limit) {
        autoAppendLimit(kylinConfig, limit, 0);
    }

    public void autoAppendLimit(KylinConfig kylinConfig, int limit, int offset) {
        if (CollectionUtils.isEmpty(allBlockList) || !isSelectStatement()) {
            return;
        }

        //Split keywords and variables from sql by punctuation and whitespace character
        List<String> sqlElements = Lists
                .newArrayList(getStatementString().toLowerCase(Locale.ROOT).split("(?![\\._\'\"`])\\p{P}|\\s+"));
        boolean limitAppended = false;

        Integer maxRows = kylinConfig.getMaxResultRows();
        if (maxRows != null && maxRows > 0 && (maxRows < limit || limit <= 0)) {
            limit = maxRows;
        }

        if (limit > 0 && !sqlElements.contains("limit")) {
            appendStmtBlock("\nLIMIT " + limit);
            limitAppended = true;
        }

        if (offset > 0 && !sqlElements.contains("offset")) {
            appendStmtBlock("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        int forceLimit;
        if ((forceLimit = kylinConfig.getForceLimit()) > 0 && !limitAppended && !sqlElements.contains("limit")
                && getStatementString().toLowerCase(Locale.ROOT).matches("^select\\s+\\*\\p{all}*")) {
            appendStmtBlock("\nLIMIT " + forceLimit);
        }
    }

    private boolean isSelectStatement() {
        String stmt = getStatementString();
        int startIndex = 0;
        char c;
        while ((c = stmt.charAt(startIndex)) == '(' || WHITE_SPACE_PATTERN.matcher(String.valueOf(c)).matches()) {
            ++startIndex;
        }
        stmt = stmt.substring(startIndex).toLowerCase(Locale.ROOT);
        return stmt.startsWith(SELECT) || (stmt.startsWith(WITH) && stmt.contains(SELECT))
                || (stmt.startsWith(EXPLAIN) && stmt.contains(SELECT));
    }

    private void removeStatementEndedSemicolon() {
        if (CollectionUtils.isEmpty(stmtBlockList)) {
            return;
        }
        RawSqlBlock block = stmtBlockList.get(stmtBlockList.size() - 1);
        String text = block.getText();
        boolean done = false;
        for (int i = text.length() - 1; !done && i >= 0; i--) {
            char c = text.charAt(i);
            if (WHITE_SPACE_PATTERN.matcher(String.valueOf(c)).matches()) {
                continue;
            }
            if (c == ';') {
                text = text.substring(0, i) + text.substring(i + 1);
            } else {
                done = true;
            }
        }
        block.setText(text);
    }

    private void appendStmtBlock(String stmt) {
        appendStmtBlock(stmt, 1);
    }

    private void appendStmtBlock(String stmt, int lines) {
        int lineNo = allBlockList.get(allBlockList.size() - 1).getEndLine() + 1;
        RawSqlBlock block = new RawSqlBlock(stmt, RawSqlBlock.Type.STATEMENT, lineNo, 0, lineNo + lines - 1,
                stmt.length());
        stmtBlockList.add(block);
        allBlockList.add(block);
        clearCache();
    }

    private void clearCache() {
        statementStringCache = null;
        fullTextStringCache = null;
    }
}
