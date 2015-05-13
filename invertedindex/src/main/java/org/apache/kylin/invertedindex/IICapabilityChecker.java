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

package org.apache.kylin.invertedindex;

import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 */
public class IICapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(IICapabilityChecker.class);

    public static boolean check(IIInstance ii, SQLDigest digest) {

        // retrieve members from olapContext
        Collection<JoinDesc> joins = digest.joinDescs;

        // match dimensions & aggregations & joins

        boolean isOnline = ii.isReady();

        boolean matchJoin = isMatchedWithJoins(joins, ii);

        if (!isOnline || !matchJoin) {
            logger.info("Exclude ii " + ii.getName() + " because " + " isOnlne=" + isOnline + ",matchJoin=" + matchJoin);
            return false;
        }

        return true;
    }

    private static boolean isMatchedWithJoins(Collection<JoinDesc> joins, IIInstance iiInstance) {
        IIDesc iiDesc = iiInstance.getDescriptor();
        List<TableDesc> tables = iiDesc.listTables();

        List<JoinDesc> cubeJoins = new ArrayList<JoinDesc>(tables.size());
        for (TableDesc tableDesc : tables) {
            JoinDesc join = null;
            for (LookupDesc lookup : iiDesc.getModel().getLookups()) {
                if (lookup.getTable().equalsIgnoreCase(tableDesc.getIdentity())) {
                    join = lookup.getJoin();
                    cubeJoins.add(join);
                    break;
                }
            }
        }

        for (JoinDesc j : joins) {
            // optiq engine can't decide which one is fk or pk
            String pTable = j.getPrimaryKeyColumns()[0].getTable();
            String factTable = iiDesc.getModel().getFactTable();
            if (factTable.equals(pTable)) {
                j.swapPKFK();
            }

            // check primary key, all PK column should refer to same tale, the Fact Table of iiInstance.
            // Using first column's table name to check.
            String fTable = j.getForeignKeyColumns()[0].getTable();
            if (!factTable.equals(fTable)) {
                logger.info("Fact Table" + factTable + " not matched in join: " + j + " on ii " + iiInstance.getName());
                return false;
            }

            // The hashcode() function of JoinDesc has been overwritten,
            // which takes into consideration: pk,fk,jointype
            if (!cubeJoins.contains(j)) {
                logger.info("Query joins don't match on ii " + iiInstance.getName());
                return false;
            }
        }
        return true;
    }

}
