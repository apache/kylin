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

package org.apache.kylin.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.calcite.avatica.InternalProperty;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kylin.common.HotLoadKylinPropertiesTestCase;
import org.apache.kylin.common.KylinConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryConnectionTest extends HotLoadKylinPropertiesTestCase {

    private static final String SQL_WITH_MIXED_CASE = "select 1 as value_ALIAS";

    @Test
    public void testGetConnection() throws SQLException, IllegalAccessException {
        Connection connection = QueryConnection.getConnection("default");
        assertNotNull(connection);
        Map<InternalProperty, Object> properties = dirtyReadProperties(connection);
        assertEquals(true, properties.get(InternalProperty.CASE_SENSITIVE));
        assertEquals(Casing.TO_UPPER, properties.get(InternalProperty.UNQUOTED_CASING));
        assertEquals(Quoting.DOUBLE_QUOTE, properties.get(InternalProperty.QUOTING));

        ResultSet resultSet = connection.prepareStatement(SQL_WITH_MIXED_CASE).executeQuery();
        String columnName = resultSet.getMetaData().getColumnName(1);
        assertEquals("VALUE_ALIAS", columnName);

        // override connection properties
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        conf.setProperty("kylin.query.calcite.extras-props.caseSensitive", "false");
        conf.setProperty("kylin.query.calcite.extras-props.unquotedCasing", "UNCHANGED");
        conf.setProperty("kylin.query.calcite.extras-props.quoting", "BRACKET");
        Connection conn2 = QueryConnection.getConnection("default");
        Map<InternalProperty, Object> props = dirtyReadProperties(conn2);
        assertEquals(false, props.get(InternalProperty.CASE_SENSITIVE));
        assertEquals(Casing.UNCHANGED, props.get(InternalProperty.UNQUOTED_CASING));
        assertEquals(Quoting.BRACKET, props.get(InternalProperty.QUOTING));

        ResultSet resultSet1 = conn2.prepareStatement(SQL_WITH_MIXED_CASE).executeQuery();
        assertEquals("value_ALIAS", resultSet1.getMetaData().getColumnName(1));
    }

    @SuppressWarnings("unchecked")
    private static Map<InternalProperty, Object> dirtyReadProperties(Connection connection) throws IllegalAccessException {
        assertTrue(connection instanceof CalciteConnection);
        return (Map<InternalProperty, Object>) FieldUtils.readField(connection, "properties");

    }
}
