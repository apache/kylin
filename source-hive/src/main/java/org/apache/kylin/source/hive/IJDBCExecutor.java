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

package org.apache.kylin.source.hive;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.kylin.metadata.model.TableDesc;

import java.io.IOException;

public interface IJDBCExecutor {
    
    void executeHQL(String hql) throws CommandNeedRetryException, IOException;

    void executeHQL(String[] hqls) throws CommandNeedRetryException, IOException;
    
    public String generateCreateSchemaSql(String schemaName);
    
    public String generateLoadDataSql(String tableName, String tableFileDir);
    
    public String[] generateCreateTableSql(TableDesc tableDesc);
    
    public String[] generateCreateViewSql(String viewName, String tableName);
}
