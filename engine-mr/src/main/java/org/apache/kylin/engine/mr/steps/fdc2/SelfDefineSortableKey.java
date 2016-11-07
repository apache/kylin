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
package org.apache.kylin.engine.mr.steps.fdc2;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Created by xiefan on 16-11-1.
 */
public class SelfDefineSortableKey implements WritableComparable<SelfDefineSortableKey> {

    private byte typeId; //non-numeric(0000 0000) int(0000 0001) other numberic(0000 0010)

    private Text text;

    private static final Logger logger = LoggerFactory.getLogger(SelfDefineSortableKey.class);

    public SelfDefineSortableKey() {
    }

    public SelfDefineSortableKey(byte typeId, Text text) {
        this.typeId = typeId;
        this.text = text;
    }

    @Override
    public int compareTo(SelfDefineSortableKey o) {
        if (!o.isNumberFamily()) {
            return this.text.compareTo(o.text);
        } else {
            byte[] data1 = this.text.getBytes();
            byte[] data2 = o.text.getBytes();
            String str1 = new String(data1, 1, data1.length - 1);
            String str2 = new String(data2, 1, data2.length - 1);
            if (str1 == null || str1.equals("") || str2 == null || str2.equals("")) {
                //should not achieve here
                logger.error("none numeric value!");
                return 0;
            }
            if (o.isIntegerFamily()) {  //integer type
                try {
                    Long num1 = Long.parseLong(str1);
                    Long num2 = Long.parseLong(str2);
                    return num1.compareTo(num2);
                } catch (NumberFormatException e) {
                    System.out.println("NumberFormatException when parse integer family number.str1:" + str1 + " str2:" + str2);
                    logger.error("NumberFormatException when parse integer family number.str1:" + str1 + " str2:" + str2);
                    e.printStackTrace();
                    return 0;
                }
            } else {  //other numeric type
                try {
                    Double num1 = Double.parseDouble(str1);
                    Double num2 = Double.parseDouble(str2);
                    return num1.compareTo(num2);
                } catch (NumberFormatException e) {
                    System.out.println("NumberFormatException when parse double family number.str1:" + str1 + " str2:" + str2);
                    logger.error("NumberFormatException when parse doul family number.str1:" + str1 + " str2:" + str2);
                    //e.printStackTrace();
                    return 0;
                }
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(typeId);
        text.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dataInput.readByte();
        text.readFields(dataInput);
    }

    public short getTypeId() {
        return typeId;
    }

    public Text getText() {
        return text;
    }

    public boolean isNumberFamily() {
        if (typeId == TypeFlag.NONE_NUMERIC_TYPE.ordinal()) return false;
        return true;
    }

    public boolean isIntegerFamily() {
        return (typeId == TypeFlag.INTEGER_FAMILY_TYPE.ordinal());
    }

    public boolean isOtherNumericFamily() {
        return (typeId == TypeFlag.DOUBLE_FAMILY_TYPE.ordinal());
    }

    public void setTypeId(byte typeId) {
        this.typeId = typeId;
    }

    public void setText(Text text) {
        this.text = text;
    }
}
