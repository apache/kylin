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

package org.apache.kylin.source.datagen;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.metadata.model.TableDesc;

public class TableGenConfig {
    
    boolean needGen;
    double rows;
    
    public TableGenConfig(TableDesc table, ModelDataGenerator modelGen) throws IOException {
        String dataGen = table.getDataGen();
        if (dataGen == null && modelGen.existsInStore(table) == false) {
            dataGen = "";
        }
        
        if (dataGen == null || "no".equals(dataGen) || "false".equals(dataGen) || "skip".equals(dataGen))
            return;
        
        if (table.isView())
            return;
        
        needGen = true;
        
        Map<String, String> config = Util.parseEqualCommaPairs(dataGen, "rows");
        
        // config.rows is either a multiplier (0,1] or an absolute row number
        rows = Util.parseDouble(config, "rows", modelGen.getModle().isFactTable(table.getIdentity()) ? 1.0 : 20);
    }
    
}
