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

package org.apache.kylin.source;

import org.apache.kylin.metadata.model.TableDesc;

/**
 * Responsible for deploying sample (CSV) data to the source database.
 */
public interface ISampleDataDeployer {

    /**
     * Create a new database (or schema) if not exists.
     */
    void createSampleDatabase(String database) throws Exception;

    /**
     * Create a new table if not exists.
     */
    void createSampleTable(TableDesc table) throws Exception;

    /**
     * Overwrite sample CSV data into a table.
     */
    void loadSampleData(String tableName, String tmpDataDir) throws Exception;

    /**
     * Create a view that wraps over a table, like "create view VIEW_NAME as select * from TABLE_NAME"
     */
    void createWrapperView(String origTableName, String viewName) throws Exception;
}
