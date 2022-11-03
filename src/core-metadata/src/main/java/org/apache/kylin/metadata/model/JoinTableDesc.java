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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinTableDesc implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String FLATTEN = "flatten";
    public static final String NORMALIZED = "normalized";

    @JsonProperty("table")
    private String table;

    @JsonProperty("kind")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private NDataModel.TableKind kind = NDataModel.TableKind.LOOKUP;

    @JsonProperty("alias")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String alias;

    @JsonProperty("join")
    private JoinDesc join;

    @JsonProperty("flattenable")
    private String flattenable;

    @JsonProperty("join_relation_type")
    private ModelJoinRelationTypeEnum joinRelationTypeEnum = ModelJoinRelationTypeEnum.MANY_TO_ONE;

    private TableRef tableRef;

    public boolean isFlattenable() {
        return !NORMALIZED.equalsIgnoreCase(this.flattenable);
    }

    public boolean isDerivedForbidden() {
        return isFlattenable() && isToManyJoinRelation();
    }

    public boolean isToManyJoinRelation() {
        return this.joinRelationTypeEnum == ModelJoinRelationTypeEnum.MANY_TO_MANY
                || this.joinRelationTypeEnum == ModelJoinRelationTypeEnum.ONE_TO_MANY;
    }

    public boolean isDerivedToManyJoinRelation() {
        return !isFlattenable() && isToManyJoinRelation();
    }

    public boolean hasDifferentAntiFlattenable(JoinTableDesc other) {
        return this.isFlattenable() ^ other.isFlattenable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        JoinTableDesc that = (JoinTableDesc) o;

        if (table != null ? !table.equals(that.table) : that.table != null)
            return false;
        if (kind != that.kind)
            return false;
        if (alias != null ? !alias.equals(that.alias) : that.alias != null)
            return false;
        if (!Objects.equals(this.joinRelationTypeEnum, that.joinRelationTypeEnum)) {
            return false;
        }
        return join != null ? join.equals(that.join) : that.join == null;
    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (kind != null ? kind.hashCode() : 0);
        result = 31 * result + (alias != null ? alias.hashCode() : 0);
        result = 31 * result + (join != null ? join.hashCode() : 0);
        result = 31 * result + Objects.hashCode(joinRelationTypeEnum);
        return result;
    }
}
