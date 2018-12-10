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

package org.apache.kylin.stream.core.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.datatype.DataType;

/**
 * Created by gwang3 on 2017/3/6.
 */
public class DataGenerator {

    public static List<String[]> generateDataForTypes(String[] types, int rowCount) {
        int len = types.length;
        List<String[]> rows = new ArrayList<>();
        for (int r = 0; r < rowCount; r++) {
            rows.add(new String[len]);
        }
        for (int i = 0; i < len; i++) {
            String[] colunmValues = generateDataForType(types[i], rowCount);
            for (int m = 0; m < rowCount; m++) {
                rows.get(m)[i] = colunmValues[m];
            }
        }
        return rows;
    }

    public static String[] generateDataForType(String type, int count) {
        String[] data = new String[count];

        if (DataType.INTEGER_FAMILY.contains(type)) {
            for (int j = 0; j < count; j++) {
                data[j] = "1";
            }
        } else if (DataType.DATETIME_FAMILY.contains(type)) {
            for (int j = 0; j < count; j++) {
                data[j] = "1";
            }
        } else if (DataType.NUMBER_FAMILY.contains(type)) {
            for (int j = 0; j < count; j++) {
                data[j] = "1";
            }
        } else if (DataType.STRING_FAMILY.contains(type)) {
            if (type.equalsIgnoreCase("char")) {
                for (int j = 0; j < count; j++) {
                    data[j] = "i";
                }
            }
            if (type.equalsIgnoreCase("varchar")) {
                for (int j = 0; j < count; j++) {
                    data[j] = "1";
                }
            } else {
                throw new IllegalArgumentException("Illegal data type: " + type);
            }
        } else {
            throw new IllegalArgumentException("Illegal data type: " + type);
        }
        return data;
    }
}
