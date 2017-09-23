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

package org.apache.kylin.cube.cuboid.algorithm.generic.lib;

/**
 * Exception to be thrown when two Chromosomes length differ.
 *
 */
public class ChromosomeMismatchException extends IllegalArgumentException {
    private static final long serialVersionUID = -7483865132286153255L;

    private final int length;

    /**
     * Construct an exception from the mismatched chromosomes.
     *
     * @param errorMsg error info.
     * @param wrong Wrong length.
     * @param expected Expected length.
     */
    public ChromosomeMismatchException(String errorMsg, int wrong, int expected) {
        super(errorMsg);
        length = expected;
    }

    /**
     * @return the expected length.
     */
    public int getLength() {
        return length;
    }
}
