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

package org.apache.kylin.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.apache.kylin.jdbc.json.SQLResponseStub;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by dongli on 1/25/16.
 */
public class SQLResonseStubTest {

    @Test
    public void testReadValuePartRecognizedField() throws IOException {
        final String payload = "{ \"columnMetas\":[ { \"isNullable\":1, \"displaySize\":0, \"schemaName\":null, \"catelogName\":null, \"tableName\":null, \"precision\":0, \"scale\":0, \"columnType\":91, \"columnTypeName\":\"DATE\", \"readOnly\":true, \"writable\":false, \"caseSensitive\":true, \"searchable\":false, \"currency\":false, \"signed\":true, \"autoIncrement\":false, \"definitelyWritable\":false }, { \"isNullable\":1, \"displaySize\":10, \"label\":\"LEAF_CATEG_ID\", \"name\":\"LEAF_CATEG_ID\", \"schemaName\":null, \"catelogName\":null, \"tableName\":null, \"precision\":10, \"scale\":0, \"columnType\":4, \"columnTypeName\":\"INTEGER\", \"readOnly\":true, \"writable\":false, \"caseSensitive\":true, \"searchable\":false, \"currency\":false, \"signed\":true, \"autoIncrement\":false, \"definitelyWritable\":false } ], \"results\":[ [ \"2013-08-07\", \"32996\", \"15\", \"15\", \"Auction\", \"10000000\", \"49.048952730908745\", \"49.048952730908745\", \"49.048952730908745\", \"1\" ], [ \"2013-08-07\", \"43398\", \"0\", \"14\", \"ABIN\", \"10000633\", \"85.78317064220418\", \"85.78317064220418\", \"85.78317064220418\", \"1\" ] ], \"cube\":\"test_kylin_cube_with_slr_desc\", \"affectedRowCount\":0, \"isException\":false, \"exceptionMessage\":null, \"duration\":3451, \"partial\":false }";
        final SQLResponseStub stub = new ObjectMapper().readValue(payload, SQLResponseStub.class);
        assertEquals("test_kylin_cube_with_slr_desc", stub.getCube());
        assertEquals(3451, stub.getDuration());
        assertFalse(stub.getColumnMetas().isEmpty());
        assertEquals(91, stub.getColumnMetas().get(0).getColumnType());
        assertNull(stub.getColumnMetas().get(0).getLabel());
        assertFalse(stub.getResults().isEmpty());
        assertNull(stub.getExceptionMessage());
    }

    @Test
    public void testReadValueWithUnrecognizedField() throws IOException {
        final String payload = "{ \"columnMetas\":[ { \"Unrecognized\":0, \"isNullable\":1, \"displaySize\":0, \"label\":\"CAL_DT\", \"name\":\"CAL_DT\", \"schemaName\":null, \"catelogName\":null, \"tableName\":null, \"precision\":0, \"scale\":0, \"columnType\":91, \"columnTypeName\":\"DATE\", \"readOnly\":true, \"writable\":false, \"caseSensitive\":true, \"searchable\":false, \"currency\":false, \"signed\":true, \"autoIncrement\":false, \"definitelyWritable\":false }, { \"isNullable\":1, \"displaySize\":10, \"label\":\"LEAF_CATEG_ID\", \"name\":\"LEAF_CATEG_ID\", \"schemaName\":null, \"catelogName\":null, \"tableName\":null, \"precision\":10, \"scale\":0, \"columnType\":4, \"columnTypeName\":\"INTEGER\", \"readOnly\":true, \"writable\":false, \"caseSensitive\":true, \"searchable\":false, \"currency\":false, \"signed\":true, \"autoIncrement\":false, \"definitelyWritable\":false } ], \"results\":[ [ \"2013-08-07\", \"32996\", \"15\", \"15\", \"Auction\", \"10000000\", \"49.048952730908745\", \"49.048952730908745\", \"49.048952730908745\", \"1\" ], [ \"2013-08-07\", \"43398\", \"0\", \"14\", \"ABIN\", \"10000633\", \"85.78317064220418\", \"85.78317064220418\", \"85.78317064220418\", \"1\" ] ], \"cube\":\"test_kylin_cube_with_slr_desc\", \"affectedRowCount\":0, \"isException\":false, \"exceptionMessage\":null, \"duration\":3451, \"partial\":false, \"hitCache\":false }";
        final SQLResponseStub stub = new ObjectMapper().readValue(payload, SQLResponseStub.class);
        assertEquals("test_kylin_cube_with_slr_desc", stub.getCube());
        assertEquals(3451, stub.getDuration());
        assertFalse(stub.getColumnMetas().isEmpty());
        assertEquals(91, stub.getColumnMetas().get(0).getColumnType());
        assertEquals("CAL_DT", stub.getColumnMetas().get(0).getLabel());
        assertFalse(stub.getResults().isEmpty());
        assertNull(stub.getExceptionMessage());
    }
}
