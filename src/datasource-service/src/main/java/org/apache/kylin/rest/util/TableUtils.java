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

package org.apache.kylin.rest.util;

import static org.apache.kylin.common.exception.ServerErrorCode.TIMESTAMP_COLUMN_NOT_EXIST;

import java.util.Arrays;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TableDesc;

import lombok.val;

public class TableUtils {
    private TableUtils() {}

    public static void checkTimestampColumn(TableDesc tableDesc) {
        val columns = tableDesc.getColumns();
        val result = Arrays.stream(columns).filter(column -> DataType.TIMESTAMP.equals(column.getDatatype())).findAny();
        if (!result.isPresent()) {
            throw new KylinException(TIMESTAMP_COLUMN_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getTimestampColumnNotExist()));
        }
    }

}
