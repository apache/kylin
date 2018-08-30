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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.junit.Assert;
import org.junit.Test;

public class StreamingControllerTest {

    @Test
    public void testReadTableDesc() throws IOException {
        String requestTableData = "{\"name\":\"my_table_name\",\"source_type\":1,\"columns\":[{\"id\":1,\"name\":"
                + "\"amount\",\"datatype\":\"decimal\"},{\"id\":2,\"name\":\"category\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":3,\"name\":\"order_time\",\"datatype\":\"timestamp\"},{\"id\":4,\"name\":\"device\","
                + "\"datatype\":\"varchar(256)\"},{\"id\":5,\"name\":\"qty\",\"datatype\":\"int\"},{\"id\":6,\"name\":"
                + "\"user_id\",\"datatype\":\"varchar(256)\"},{\"id\":7,\"name\":\"user_age\",\"datatype\":\"int\"},"
                + "{\"id\":8,\"name\":\"user_gender\",\"datatype\":\"varchar(256)\"},{\"id\":9,\"name\":\"currency\","
                + "\"datatype\":\"varchar(256)\"},{\"id\":10,\"name\":\"country\",\"datatype\":\"varchar(256)\"},"
                + "{\"id\":11,\"name\":\"year_start\",\"datatype\":\"date\"},{\"id\":12,\"name\":\"quarter_start\","
                + "\"datatype\":\"date\"},{\"id\":13,\"name\":\"month_start\",\"datatype\":\"date\"},{\"id\":14,"
                + "\"name\":\"week_start\",\"datatype\":\"date\"},{\"id\":15,\"name\":\"day_start\",\"datatype\":"
                + "\"date\"},{\"id\":16,\"name\":\"hour_start\",\"datatype\":\"timestamp\"},{\"id\":17,\"name\":"
                + "\"minute_start\",\"datatype\":\"timestamp\"}],\"database\":\"my_database_name\"}";
        TableDesc desc = JsonUtil.readValue(requestTableData, TableDesc.class);
        String[] dbTable = HadoopUtil.parseHiveTableName(desc.getIdentity());
        desc.setName(dbTable[1]);
        desc.setDatabase(dbTable[0]);
        Assert.assertEquals("my_table_name".toUpperCase(Locale.ROOT), desc.getName());
        Assert.assertEquals("my_database_name".toUpperCase(Locale.ROOT), desc.getDatabase());
    }
}
