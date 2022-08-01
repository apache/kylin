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

/**
 * @author neng.liu
 */
public class ColumnWithAlias {
    private final boolean distinct;
    private final String alias;
    private final String name;
    private final String expr;

    private ColumnWithAlias(boolean distinct, String alias, String name, String expr) {
        this.distinct = distinct;
        this.alias = alias;
        this.name = name;
        this.expr = expr;
    }

    public static ColumnWithAliasBuilder builder() {
        return new ColumnWithAliasBuilder();
    }

    public static class ColumnWithAliasBuilder {

        private boolean distinct;
        private String alias;
        private String name;
        private String expr;

        public ColumnWithAliasBuilder distinct(boolean distinct) {
            this.distinct = distinct;
            return this;
        }

        public ColumnWithAliasBuilder alias(String alias) {
            this.alias = alias;
            return this;
        }

        public ColumnWithAliasBuilder name(String name) {
            this.name = name;
            return this;
        }

        public ColumnWithAliasBuilder expr(String expr) {
            this.expr = expr;
            return this;
        }

        public ColumnWithAlias build() {
            return new ColumnWithAlias(distinct, alias, name, expr);
        }
    }

    public boolean isDistinct() {
        return distinct;
    }

    public String getAlias() {
        return alias;
    }

    public String getName() {
        return name;
    }

    public String getExpr() {
        return expr;
    }
}
