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

package org.apache.kylin.cube;

import static org.junit.Assert.assertNotNull;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CubeInstanceTest {

    private CubeInstance cubeInstance;

    @Before
    public void setUp() throws Exception {
        InputStream fileInputStream = new FileInputStream("src/test/resources/learn_kylin_cube2.json");
        JsonSerializer<CubeInstance> jsonSerializer = new JsonSerializer<>(CubeInstance.class);
        cubeInstance = jsonSerializer.deserialize(new DataInputStream(fileInputStream));
    }

    @Test
    public void getTreeCuboidSchedulerTest() {
        Map<Long, Long> cuboidsWithRowCnt = cubeInstance.getCuboids();
        TreeCuboidScheduler.CuboidTree.createFromCuboids(Lists.newArrayList(cuboidsWithRowCnt.keySet()),
                new TreeCuboidScheduler.CuboidCostComparator(cuboidsWithRowCnt));

        List<Long> cuboids = Lists.newArrayList(cuboidsWithRowCnt.keySet());
        Collections.sort(cuboids);
        for (Long cuboid : cuboids) {
            System.out.println(cuboid + ":" + cuboidsWithRowCnt.get(cuboid));
        }
        assertNotNull(cuboidsWithRowCnt.get(255L));
    }
}
