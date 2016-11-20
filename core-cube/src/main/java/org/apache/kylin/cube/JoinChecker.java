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

package org.apache.kylin.cube;

import java.util.Collection;

import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.realization.IRealization;

public class JoinChecker {
    
    // given ModelChooser has done the model join matching already, this method seems useless
    public static boolean isJoinMatch(Collection<JoinDesc> joins, IRealization realization) {
//        List<JoinDesc> realizationsJoins = Lists.newArrayList();
//        for (JoinTableDesc joinTable : realization.getModel().getJoinTables()) {
//            realizationsJoins.add(joinTable.getJoin());
//        }
//
//        for (JoinDesc j : joins) {
//            // optiq engine can't decide which one is fk or pk
//            String pTable = j.getPrimaryKeyColumns()[0].getTable();
//            String factTable = realization.getModel().getRootFactTable().getTableIdentity();
//            if (factTable.equals(pTable)) {
//                j.swapPKFK();
//            }
//
//            // check primary key, all PK column should refer to same tale, the Fact Table of cube.
//            // Using first column's table name to check.
//            String fTable = j.getForeignKeyColumns()[0].getTable();
//            if (!factTable.equals(fTable)) {
//                logger.info("Fact Table" + factTable + " not matched in join: " + j + " on cube " + realization.getName());
//                return false;
//            }
//
//            // The hashcode() function of JoinDesc has been overwritten,
//            // which takes into consideration: pk,fk,jointype
//            if (!realizationsJoins.contains(j)) {
//                logger.info("Query joins don't macth on cube " + realization.getName());
//                return false;
//            }
//        }
        return true;
    }
}