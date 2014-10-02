/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.cube.cli;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.dict.TrieDictionary;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.TblColRef;

import java.io.IOException;
import java.util.Set;

public class DumpDictionaryCLI {

    @SuppressWarnings({"rawtypes"})
    public static void main(String[] args) throws IOException {
        String inpMetaFolder = args[0]; // metadata folder
        String inpCubeName = args[1]; // cube name

        inpCubeName = inpCubeName.toUpperCase();

        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.createInstanceFromUri(inpMetaFolder));
        CubeInstance cube = cubeMgr.getCube(inpCubeName);
        //CubeSegment seg = cube.getTheOnlySegment();
        CubeDesc cubeDesc = cube.getDescriptor();
        Set<TblColRef> allColumns = cubeDesc.listAllColumns();

        for (CubeSegment segment : cube.getSegments()) {
            for (TblColRef col : allColumns) {
                Dictionary dictionary = cubeMgr.getDictionary(segment, col);
                if (!(dictionary instanceof TrieDictionary))
                    continue;
                TrieDictionary dict = (TrieDictionary) dictionary;
                System.out
                        .println("=========================================================================");
                System.out.println("Dump dict of " + col + " in segment " + segment.getName());
                dict.dump(System.out);
            }
        }
    }
}
