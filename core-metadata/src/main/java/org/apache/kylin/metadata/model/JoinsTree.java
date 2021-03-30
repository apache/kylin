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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class JoinsTree implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final IJoinDescMatcher DEFAULT_JOINDESC_MATCHER = new DefaultJoinDescMatcher();

    private Map<String, Chain> tableChains = new LinkedHashMap<>();
    private IJoinDescMatcher joinDescMatcher = DEFAULT_JOINDESC_MATCHER;

    public JoinsTree(TableRef rootTable, List<JoinDesc> joins) {
        for (JoinDesc join : joins) {
            for (TblColRef col : join.getForeignKeyColumns())
                Preconditions.checkState(col.isQualified());
            for (TblColRef col : join.getPrimaryKeyColumns())
                Preconditions.checkState(col.isQualified());
        }

        tableChains.put(rootTable.getAlias(), new Chain(rootTable, null, null));
        for (JoinDesc join : joins) {
            TableRef pkSide = join.getPKSide();
            Chain fkSide = tableChains.get(join.getFKSide().getAlias());
            tableChains.put(pkSide.getAlias(), new Chain(pkSide, join, fkSide));
        }
    }

    public JoinsTree(Map<String, Chain> tableChains) {
        this.tableChains = tableChains;
    }

    public Map<String, String> matches(JoinsTree another) {
        return matches(another, Collections.<String, String> emptyMap());
    }

    public Map<String, String> matches(JoinsTree another, Map<String, String> constraints) {
        Map<String, String> matchUp = new HashMap<>();

        for (Chain chain : tableChains.values()) {
            if (matchInTree(chain, another, constraints, matchUp) == false)
                return null;
        }

        return matchUp;
    }

    public int matchNum(JoinsTree another) {
        Map<String, String> matchUp = new HashMap<>();

        for (Chain chain : tableChains.values()) {
            matchInTree(chain, another, Collections.<String, String> emptyMap(), matchUp);
        }

        return matchUp.size();
    }

    private boolean matchInTree(Chain chain, JoinsTree another, Map<String, String> constraints,
            Map<String, String> matchUp) {
        String thisAlias = chain.table.getAlias();
        if (matchUp.containsKey(thisAlias))
            return true;

        String constraint = constraints.get(thisAlias);
        if (constraint != null) {
            return matchChain(chain, another.tableChains.get(constraint), matchUp);
        }

        for (Chain anotherChain : another.tableChains.values()) {
            if (matchChain(chain, anotherChain, matchUp)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchChain(Chain chain, Chain anotherChain, Map<String, String> matchUp) {
        String thisAlias = chain.table.getAlias();
        String anotherAlias = anotherChain.table.getAlias();

        String curMatch = matchUp.get(thisAlias);
        if (curMatch != null)
            return curMatch.equals(anotherAlias);
        if (curMatch == null && matchUp.values().contains(anotherAlias))
            return false;

        boolean matches = false;
        if (chain.join == null) {
            matches = anotherChain.join == null
                    && chain.table.getTableDesc().getIdentity().equals(anotherChain.table.getTableDesc().getIdentity());
        } else {
            matches = joinDescMatcher.matches(chain.join, anotherChain.join)
                    && matchChain(chain.fkSide, anotherChain.fkSide, matchUp);
        }

        if (matches) {
            matchUp.put(thisAlias, anotherAlias);
        }
        return matches;
    }

    public JoinDesc getJoinByPKSide(TableRef table) {
        Chain chain = tableChains.get(table.getAlias());
        if (chain == null)
            return null;
        else
            return chain.join;
    }

    public List<Chain> unmatchedChain(JoinsTree another, Map<String, String> constraints) {
        Map<String, String> matchUp = new HashMap<>();
        List<Chain> unmatchedChainList = Lists.newArrayList();
        for (Chain chain : tableChains.values()) {
            if (matchInTree(chain, another, constraints, matchUp) == false)
                unmatchedChainList.add(chain);
        }

        return unmatchedChainList;
    }

    public Map<String, Chain> getTableChains() {
        return tableChains;
    }

    public void setJoinDescMatcher(IJoinDescMatcher joinDescMatcher) {
        this.joinDescMatcher = joinDescMatcher;
    }

    public JoinsTree getSubgraphByAlias(Set<String> aliases) {
        Map<String, Chain> subgraph = Maps.newHashMap();
        for (String alias : aliases) {
            Chain chain = tableChains.get(alias);
            if (chain == null)
                throw new IllegalArgumentException("Table with alias " + alias + " is not found");

            while (chain.getFkSide() != null) {
                subgraph.put(chain.table.getAlias(), chain);
                chain = chain.getFkSide();
            }
            subgraph.put(chain.table.getAlias(), chain);
        }
        return new JoinsTree(subgraph);
    }

    public static class Chain implements Serializable {
        private static final long serialVersionUID = 1L;

        TableRef table; // pk side
        JoinDesc join;
        Chain fkSide;

        public Chain(TableRef table, JoinDesc join, Chain fkSide) {
            this.table = table;
            this.join = join;
            this.fkSide = fkSide;
            if (join != null) {
                Preconditions.checkArgument(table == join.getPKSide());
                Preconditions.checkArgument(fkSide.table == join.getFKSide());
            }
        }

        public JoinDesc getJoin() {
            return join;
        }

        public TableRef getTable() {
            return table;
        }

        public Chain getFkSide() {
            return fkSide;
        }
    }

    public static interface IJoinDescMatcher {
        boolean matches(JoinDesc join1, JoinDesc join2);
    }

    public static class DefaultJoinDescMatcher implements IJoinDescMatcher, Serializable {
        @Override
        public boolean matches(JoinDesc join1, JoinDesc join2) {
            if (join1 == null) {
                return join2 == null;
            } else if (join2 == null) {
                return false;
            } else {

                if (!join1.getType().equalsIgnoreCase(join2.getType()))
                    return false;

                // note pk/fk are sorted, sortByFK()
                if (!this.columnDescEquals(join1.getForeignKeyColumns(), join2.getForeignKeyColumns()))
                    return false;
                if (!this.columnDescEquals(join1.getPrimaryKeyColumns(), join2.getPrimaryKeyColumns()))
                    return false;

                return true;
            }
        }

        private boolean columnDescEquals(TblColRef[] a, TblColRef[] b) {
            if (a.length != b.length)
                return false;

            for (int i = 0; i < a.length; i++) {
                if (columnDescEquals(a[i].getColumnDesc(), b[i].getColumnDesc()) == false)
                    return false;
            }
            return true;
        }

        protected boolean columnDescEquals(ColumnDesc a, ColumnDesc b) {
            return a == null ? b == null : a.equals(b);
        }
    }
}
