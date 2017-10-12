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


package org.apache.kylin.cube.inmemcubing2;

import java.io.IOException;
import java.util.concurrent.RecursiveTask;

import org.apache.kylin.cube.inmemcubing.CuboidResult;

@SuppressWarnings("serial")
class CuboidTask extends RecursiveTask<CuboidResult> implements Comparable<CuboidTask> {
    final CuboidResult parent;
    final long childCuboidId;
    final InMemCubeBuilder2 cubeBuilder;
    
    CuboidTask(CuboidResult parent, long childCuboidId, InMemCubeBuilder2 cubeBuilder) {
        this.parent = parent;
        this.childCuboidId = childCuboidId;
        this.cubeBuilder = cubeBuilder;
    }

    @Override
    public int compareTo(CuboidTask o) {
        long comp = this.childCuboidId - o.childCuboidId;
        return comp < 0 ? -1 : (comp > 0 ? 1 : 0);
    }

    @Override
    protected CuboidResult compute() {
        try {
            return cubeBuilder.buildCuboid(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
