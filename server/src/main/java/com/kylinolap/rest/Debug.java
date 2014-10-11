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

package com.kylinolap.rest;

import java.io.IOException;
import java.util.Random;

/**
 * @author yangli9
 * 
 */
public class Debug {

    public static void main(String[] args) throws IOException {
        // MetadataManager mgr =
        // MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        // CubeDesc cubeDesc = mgr.getCubeDesc("geox_trans_mtrc_sd_cube_desc");
        //
        // int mathCalcCuboidCount = CuboidCLI.mathCalcCuboidCount(cubeDesc);
        // System.out.println(mathCalcCuboidCount);

        String[] codeSpace = new String[256 * 256 * 256];

        System.out.println("loading...");
        byte[] bytes = new byte[3];
        int p = 0;
        for (int i = 0; i < 256; i++) {
            for (int j = 0; j < 256; j++) {
                for (int k = 0; k < 256; k++) {
                    bytes[0] = (byte) i;
                    bytes[1] = (byte) j;
                    bytes[2] = (byte) k;
                    codeSpace[p] = new String(bytes, "ISO-8859-1");
                    if (codeSpace[p].length() != 3)
                        throw new IllegalStateException();
                    p++;
                }
            }
        }

        System.out.println("testing...");
        Random rand = new Random();
        while (true) {
            int a = rand.nextInt(codeSpace.length);
            int b = rand.nextInt(codeSpace.length);
            int comp = codeSpace[a].compareTo(codeSpace[b]);
            boolean ok;
            if (a == b)
                ok = comp == 0;
            else if (a < b)
                ok = comp < 0;
            else
                ok = comp > 0;
            if (!ok)
                throw new IllegalStateException();
        }
    }

}
