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

package org.apache.kylin.cube.inmemcubing;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.gridtable.GTRecord;

public class InputConverterUnitForBaseCuboid implements InputConverterUnit<ByteArray> {

    public static final ByteArray END_ROW = new ByteArray();
    public static final ByteArray CUT_ROW = new ByteArray(0);

    private final boolean ifChange;

    public InputConverterUnitForBaseCuboid(boolean ifChange) {
        this.ifChange = ifChange;
    }

    @Override
    public void convert(ByteArray currentObject, GTRecord record) {
        record.loadColumns(currentObject.asBuffer());
    }

    @Override
    public boolean ifEnd(ByteArray currentObject) {
        return currentObject == END_ROW;
    }

    @Override
    public ByteArray getEndRow() {
        return END_ROW;
    }

    @Override
    public ByteArray getCutRow() {
        return CUT_ROW;
    }

    @Override
    public boolean ifCut(ByteArray currentObject) {
        return currentObject == CUT_ROW;
    }

    @Override
    public boolean ifChange() {
        return ifChange;
    }
}