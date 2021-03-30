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

import java.util.List;
import java.util.Locale;

import org.apache.kylin.dimension.BooleanDimEnc;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.FixedLenDimEnc;
import org.apache.kylin.dimension.FixedLenHexDimEnc;
import org.apache.kylin.dimension.IntegerDimEnc;
import org.apache.kylin.dimension.TimeDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

@Component("encodingService")
public class EncodingService extends BasicService {

    public List<String> getValidEncodings(DataType dataType) {
        Message msg = MsgPicker.getMsg();

        if (dataType.isIntegerFamily()) {
            return Lists.newArrayList(BooleanDimEnc.ENCODING_NAME, DateDimEnc.ENCODING_NAME, TimeDimEnc.ENCODING_NAME,
                    DictionaryDimEnc.ENCODING_NAME, IntegerDimEnc.ENCODING_NAME);
        } else if (dataType.isNumberFamily()) { //numbers include integers
            return Lists.newArrayList(DictionaryDimEnc.ENCODING_NAME);
        } else if (dataType.isDateTimeFamily()) {
            return Lists.newArrayList(DateDimEnc.ENCODING_NAME, TimeDimEnc.ENCODING_NAME,
                    DictionaryDimEnc.ENCODING_NAME);
        } else if (dataType.isStringFamily()) {
            return Lists.newArrayList(BooleanDimEnc.ENCODING_NAME, DictionaryDimEnc.ENCODING_NAME,
                    FixedLenDimEnc.ENCODING_NAME, //
                    FixedLenHexDimEnc.ENCODING_NAME, IntegerDimEnc.ENCODING_NAME);
        } else {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getVALID_ENCODING_NOT_AVAILABLE(), dataType));
        }
    }

}
