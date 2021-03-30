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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;

import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InputConverterUnit;
import org.apache.kylin.cube.inmemcubing.InputConverterUnitForRawData;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.MRUtil;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class InMemCuboidMapper<KEYIN>
        extends InMemCuboidMapperBase<KEYIN, Object, ByteArrayWritable, ByteArrayWritable, String[]> {

    private IMRInput.IMRTableInputFormat flatTableInputFormat;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.doSetup(context);

        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
    }

    @Override
    protected InputConverterUnit<String[]> getInputConverterUnit(Context context) {
        Preconditions.checkNotNull(cubeDesc);
        Preconditions.checkNotNull(dictionaryMap);
        return new InputConverterUnitForRawData(cubeDesc, flatDesc, dictionaryMap);
    }

    @Override
    protected String[] getRecordFromKeyValue(KEYIN key, Object value) {
        return flatTableInputFormat.parseMapperInput(value).iterator().next();
    }

    @Override
    protected ICuboidWriter getCuboidWriter(Context context) {
        return new MapContextGTRecordWriter(context, cubeDesc, cubeSegment);
    }
}
