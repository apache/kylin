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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

/**
 * @author xjiang
 */
@SuppressWarnings("serial")
public class DatabaseDesc implements Serializable {
    private String name;

    /**
     * @return the name
     */
    public String getName() {
        return name == null ? "null" : name.toUpperCase(Locale.ROOT);
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "DatabaseDesc [name=" + name + "]";
    }

    public static HashMap<String, Integer> extractDatabaseOccurenceCounts(Collection<TableDesc> tables) {
        HashMap<String, Integer> databaseCounts = new HashMap<String, Integer>();
        for (TableDesc tableDesc : tables) {
            String databaseName = tableDesc.getDatabase();
            Integer counter = databaseCounts.get(databaseName);
            if (counter != null)
                databaseCounts.put(databaseName, counter + 1);
            else
                databaseCounts.put(databaseName, 1);
        }
        return databaseCounts;
    }

    public static HashSet<String> extractDatabaseNames(List<TableDesc> tables) {
        HashSet<String> databaseNames = new HashSet<String>();
        for (TableDesc tableDesc : tables) {
            databaseNames.add(tableDesc.getDatabase());
        }
        return databaseNames;
    }
}
