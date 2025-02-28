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

package org.apache.kylin.rest.service;

import java.io.IOException;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.request.SQLBlacklistItemRequest;
import org.apache.kylin.rest.request.SQLBlacklistRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo
class QuerySQLBlacklistServiceTest {

    private final QuerySQLBlacklistService service = new QuerySQLBlacklistService();

    private SQLBlacklistRequest blacklistRequest(SQLBlacklistItemRequest... items) {
        val req = new SQLBlacklistRequest();
        req.setProject("default");
        req.setBlacklistItems(Lists.newArrayList(items));
        return req;
    }

    private SQLBlacklistItemRequest itemRequest(String id, String regex, String sql, int concurrentLimit) {
        val item = new SQLBlacklistItemRequest();
        item.setId(id);
        item.setRegex(regex);
        item.setSql(sql);
        item.setConcurrentLimit(concurrentLimit);
        return item;
    }

    @Test
    public void testCrud() throws IOException {
        // save
        val saveRet = service
                .saveSqlBlacklist(blacklistRequest(itemRequest(null, "a", "b", 8), itemRequest(null, "c", "d", 8)));
        Assertions.assertNotNull(saveRet);
        Assertions.assertEquals("default", saveRet.getProject());
        Assertions.assertEquals(2, saveRet.getBlacklistItems().size());
        String id1 = saveRet.getBlacklistItems().get(0).getId();
        String id2 = saveRet.getBlacklistItems().get(1).getId();

        // get
        val getRet = service.getItemById("default", itemRequest(id1, null, null, 0));
        Assertions.assertEquals(id1, getRet.getId());
        Assertions.assertEquals("a", getRet.getRegex());
        Assertions.assertEquals("b", getRet.getSql());
        Assertions.assertEquals(8, getRet.getConcurrentLimit());

        val getRet1 = service.getItemByRegex("default", itemRequest(null, "a", null, 0));
        Assertions.assertEquals(id1, getRet1.getId());
        Assertions.assertEquals("a", getRet1.getRegex());
        Assertions.assertEquals("b", getRet1.getSql());
        Assertions.assertEquals(8, getRet1.getConcurrentLimit());

        val getRet2 = service.getItemBySql("default", itemRequest(null, null, "b", 0));
        Assertions.assertEquals(id1, getRet2.getId());
        Assertions.assertEquals("a", getRet2.getRegex());
        Assertions.assertEquals("b", getRet2.getSql());
        Assertions.assertEquals(8, getRet2.getConcurrentLimit());

        // add
        val addRet = service.addSqlBlacklistItem("default", itemRequest(null, "e", "f", 9));
        Assertions.assertNotNull(addRet);
        Assertions.assertEquals("default", addRet.getProject());
        Assertions.assertEquals(3, addRet.getBlacklistItems().size());
        String id3 = addRet.getBlacklistItems().get(2).getId();

        val getRet3 = service.getItemById("default", itemRequest(id3, null, null, 0));
        Assertions.assertEquals(id3, getRet3.getId());
        Assertions.assertEquals("e", getRet3.getRegex());
        Assertions.assertEquals("f", getRet3.getSql());
        Assertions.assertEquals(9, getRet3.getConcurrentLimit());

        // delete
        service.deleteSqlBlacklistItem("default", id3);
        Assertions.assertNull(service.getItemById("default", itemRequest(id3, null, null, 0)));

        // clear
        service.clearSqlBlacklist("default");
        Assertions.assertNull(service.getItemById("default", itemRequest(id1, null, null, 0)));
        Assertions.assertNull(service.getItemById("default", itemRequest(id2, null, null, 0)));
        Assertions.assertNull(service.getItemById("default", itemRequest(id3, null, null, 0)));
    }

    @Test
    public void testConflict() throws IOException {
        // save
        val saveRet = service.saveSqlBlacklist(blacklistRequest(itemRequest(null, "a", "b", 8), //
                itemRequest(null, "c", "d", 8)));
        Assertions.assertNotNull(saveRet);
        Assertions.assertEquals("default", saveRet.getProject());
        Assertions.assertEquals(2, saveRet.getBlacklistItems().size());
        String id1 = saveRet.getBlacklistItems().get(0).getId();

        // regex
        Assertions.assertNotNull(service.checkConflictRegex("default", itemRequest(id1, "c", null, 0)));

        // sql
        Assertions.assertNotNull(service.checkConflictSql("default", itemRequest(id1, null, "d", 0)));
    }

}
