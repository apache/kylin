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

package org.apache.kylin.externalCatalog.api.catalog;

import org.apache.kylin.externalCatalog.api.annotation.InterfaceStability.Evolving;
import java.util.Map;
import java.util.Objects;

@Evolving
public class Partition {

    private Map<String, String> partitons;

    public Partition(Map<String, String> partitons) {
        this.partitons = partitons;
    }

    public Map<String, String> getPartitions() {
        return partitons;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Partition partition = (Partition) o;
        return Objects.equals(partitons, partition.partitons);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitons);
    }

    @Override
    public String toString() {
        return "Partition{parameters=" + partitons + '}';
    }

}
