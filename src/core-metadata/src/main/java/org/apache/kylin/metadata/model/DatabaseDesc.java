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
import java.util.List;
import java.util.Map;

import org.apache.kylin.metadata.project.ProjectInstance;

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
        return name == null ? "null" : name;
    }

    /**
     * @param name the name to set
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

    public static String getDefaultDatabaseByMaxTables(Map<String, List<TableDesc>> schemaMap) {
        String majoritySchemaName = ProjectInstance.DEFAULT_DATABASE;
        int majoritySchemaCount = 0;
        for (Map.Entry<String, List<TableDesc>> e : schemaMap.entrySet()) {
            if (e.getKey().equalsIgnoreCase(ProjectInstance.DEFAULT_DATABASE)) {
                majoritySchemaName = e.getKey();
                break;
            }

            if (e.getValue().size() >= majoritySchemaCount) {
                majoritySchemaCount = e.getValue().size();
                majoritySchemaName = e.getKey();
            }
        }

        return majoritySchemaName;
    }
}
