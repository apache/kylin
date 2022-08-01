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

import java.util.Objects;

/**
 * Supported primitive type:
 * <ul>
 *     <li>{@code boolean}</li>
 *     <li>{@code tinyint}, {@code byte}</li>
 *     <li>{@code smallint}, {@code short}</li>
 *     <li>{@code int}, {@code integer}</li>
 *     <li>{@code bigint}, {@code long}</li>
 *     <li>{@code float}</li>
 *     <li>{@code date}</li>
 *     <li>{@code timestamp}</li>
 *     <li>{@code string}, {@code char(n)}, {@code varchar(n)}</li>
 *     <li>{@code binary}</li>
 *     <li>{@code decimal}</li>
 * </ul>
 * Supported complex type: map, array, struct.
 */
@Evolving
public class FieldSchema {
    private String name;    // name of the field
    private String type;    // type of the field.
    private String comment;

    public FieldSchema(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldSchema that = (FieldSchema) o;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment);
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", comment='" + comment + '\''
                + '}';
    }
}
