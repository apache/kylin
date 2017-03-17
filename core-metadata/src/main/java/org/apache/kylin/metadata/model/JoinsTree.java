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
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

public class JoinsTree  implements Serializable {
    private static final long serialVersionUID = 1L;
    
    final Map<String, Chain> tableChains = new LinkedHashMap<>();

    public JoinsTree(TableRef rootTable, List<JoinDesc> joins) {
        for (JoinDesc join : joins) {
            for (TblColRef col : join.getForeignKeyColumns())
                Preconditions.checkState(col.isQualified());
            for (TblColRef col : join.getPrimaryKeyColumns())
                Preconditions.checkState(col.isQualified());
        }

        // Walk through joins to build FK table to joins mapping
        HashMap<String, List<JoinDesc>> fkJoinMap = Maps.newHashMap();
        int joinCount = 0;
        for (JoinDesc join: joins) {
            joinCount++;
            String fkSideAlias = join.getFKSide().getAlias();
            if (fkJoinMap.containsKey(fkSideAlias)) {
                fkJoinMap.get(fkSideAlias).add(join);
            } else {
                List<JoinDesc> joinDescList = Lists.newArrayList(join);
                fkJoinMap.put(fkSideAlias, joinDescList);
            }
        }

        // Width-first build tree (tableChains)
        Queue<Chain> chainBuff = new ArrayDeque<Chain>();
        chainBuff.add(new Chain(rootTable, null, null));
        int chainCount = 0;
        while (!chainBuff.isEmpty()) {
            Chain chain= chainBuff.poll();
            String pkSideAlias = chain.table.getAlias();
            chainCount++;
            tableChains.put(pkSideAlias, chain);

            // this round pk side is next round's fk side
            if (fkJoinMap.containsKey(pkSideAlias)) {
                for (JoinDesc join: fkJoinMap.get(pkSideAlias)) {
                    chainBuff.add(new Chain(join.getPKSide(), join, chain));
                }
            }
        }

        // if join count not match (chain count - 1), there must be some join not take effect
        if (joinCount != (chainCount - 1)) {
            throw new IllegalArgumentException("There's some illegal Joins, please check your model");
        }
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

    private boolean matchInTree(Chain chain, JoinsTree another, Map<String, String> constraints, Map<String, String> matchUp) {
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
            matches = anotherChain.join == null && chain.table.getTableDesc().equals(anotherChain.table.getTableDesc());
        } else {
            matches = chain.join.matches(anotherChain.join) && matchChain(chain.fkSide, anotherChain.fkSide, matchUp);
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
    }

}
