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

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.jdbc.Driver;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.schema.OLAPSchemaFactory;

public class QueryConnection {
    
    private static Boolean isRegister = false;

    public static Connection getConnection(String project) throws SQLException {
        if (!isRegister) {
            try {
                Class<?> aClass = Thread.currentThread().getContextClassLoader()
                        .loadClass("org.apache.calcite.jdbc.Driver");
                Driver o = (Driver) aClass.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(o);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                e.printStackTrace();
            }
            isRegister = true;
        }
        File olapTmp = OLAPSchemaFactory.createTempOLAPJson(project, KylinConfig.getInstanceFromEnv());
        Properties info = new Properties();
        info.putAll(KylinConfig.getInstanceFromEnv().getCalciteExtrasProperties());
        // Import calcite props from jdbc client(override the kylin.properties)
        info.putAll(BackdoorToggles.getJdbcDriverClientCalciteProps());
        info.put("model", olapTmp.getAbsolutePath());
        info.put("typeSystem", "org.apache.kylin.query.calcite.KylinRelDataTypeSystem");
        return DriverManager.getConnection("jdbc:calcite:", info);
    }
}
