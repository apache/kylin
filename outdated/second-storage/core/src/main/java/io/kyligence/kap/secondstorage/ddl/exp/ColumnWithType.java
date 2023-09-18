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
package io.kyligence.kap.secondstorage.ddl.exp;

import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;
import io.kyligence.kap.secondstorage.ddl.visitor.Renderable;

public class ColumnWithType implements Renderable {

    private final String name;
    private final String type;
    private boolean quote;
    private final boolean nullable;

    public ColumnWithType(String name, String type) {
        this(name, type, false, false);
    }

    public ColumnWithType(String name, String type, boolean nullable) {
        this(name, type, nullable, false);
    }

    public ColumnWithType(String name, String type, boolean nullable, boolean quote) {
        this.name = name;
        this.type = type;
        this.quote = quote;
        this.nullable = nullable;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public boolean quote() {
        return quote;
    }

    public boolean nullable() {
        return nullable;
    }

    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }
}
