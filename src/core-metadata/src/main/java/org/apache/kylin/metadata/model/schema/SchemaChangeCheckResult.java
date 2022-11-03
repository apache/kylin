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
package org.apache.kylin.metadata.model.schema;

import static lombok.AccessLevel.PRIVATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.metadata.model.TableDesc;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

@Data
public class SchemaChangeCheckResult {

    @JsonProperty
    private Map<String, ModelSchemaChange> models = new HashMap<>();

    @JsonIgnore
    private List<TableDesc> existTableList = new ArrayList<>();

    @Data
    public static class ModelSchemaChange {
        private int differences;

        @Setter(PRIVATE)
        @JsonProperty("missing_items")
        private List<ChangedItem> missingItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("new_items")
        private List<ChangedItem> newItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("update_items")
        private List<UpdatedItem> updateItems = new ArrayList<>();

        @Setter(PRIVATE)
        @JsonProperty("reduce_items")
        private List<ChangedItem> reduceItems = new ArrayList<>();

        @JsonProperty("importable")
        public boolean importable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isImportable);
        }

        @JsonProperty("creatable")
        public boolean creatable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isCreatable);
        }

        @JsonProperty("")
        public boolean overwritable() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isOverwritable);
        }

        public int getDifferences() {
            return missingItems.size() + newItems.size() + updateItems.size() + reduceItems.size();
        }

        @JsonProperty("has_same_name")
        public boolean hasSameName() {
            return Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .allMatch(BaseItem::isHasSameName);
        }

        @JsonProperty("has_same_name_broken")
        public boolean hasSameNameBroken() {
            val set = Stream.of(missingItems, newItems, updateItems, reduceItems).flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            return !set.isEmpty() && set.stream().allMatch(BaseItem::isHasSameNameBroken);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BaseItem {
        @JsonProperty("type")
        SchemaNodeType type;

        @JsonProperty("model_alias")
        String modelAlias;

        @JsonUnwrapped
        ConflictReason conflictReason;

        @JsonProperty("has_same_name")
        boolean hasSameName;

        @JsonProperty("has_same_name_broken")
        boolean hasSameNameBroken;

        @JsonProperty("importable")
        boolean importable;
        @JsonProperty("creatable")
        boolean creatable;
        @JsonProperty("overwritable")
        boolean overwritable;

        @JsonIgnore
        public String getDetail(SchemaNode schemaNode) {
            switch (schemaNode.getType()) {
            case TABLE_COLUMN:
                return schemaNode.getKey();
            case MODEL_DIMENSION:
            case MODEL_MEASURE:
                return (String) schemaNode.getAttributes().get("name");
            default:
                return schemaNode.getDetail();
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ChangedItem extends BaseItem {

        private SchemaNode schemaNode;

        public ChangedItem(SchemaNodeType type, SchemaNode schemaNode, String modelAlias, UN_IMPORT_REASON reason,
                String conflictItem, BaseItemParameter parameter) {
            super(type, modelAlias, new ConflictReason(reason, conflictItem), parameter.hasSameName,
                    parameter.hasSameNameBroken, parameter.importable, parameter.creatable, parameter.overwritable);
            this.schemaNode = schemaNode;
        }

        public static ChangedItem createUnImportableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                UN_IMPORT_REASON reason, String conflictItem, boolean hasSameName, boolean hasSameNameBroken) {
            return new ChangedItem(type, schemaNode, null, reason, conflictItem,
                    new BaseItemParameter(hasSameName, hasSameNameBroken, false, false, false));
        }

        public static ChangedItem createUnImportableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                String modelAlias, UN_IMPORT_REASON reason, String conflictItem, boolean hasSameName,
                boolean hasSameNameBroken) {
            return new ChangedItem(type, schemaNode, modelAlias, reason, conflictItem,
                    new BaseItemParameter(hasSameName, hasSameNameBroken, false, false, false));
        }

        public static ChangedItem createOverwritableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                boolean hasSameName, boolean hasSameNameBroken) {
            return new ChangedItem(type, schemaNode, null, null, null,
                    new BaseItemParameter(hasSameName, hasSameNameBroken, true, true, true));
        }

        public static ChangedItem createOverwritableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                String modelAlias, boolean hasSameName, boolean hasSameNameBroken) {
            return new ChangedItem(type, schemaNode, modelAlias, null, null,
                    new BaseItemParameter(hasSameName, hasSameNameBroken, true, true, true));
        }

        public static ChangedItem createCreatableSchemaNode(SchemaNodeType type, SchemaNode schemaNode,
                boolean hasSameName, boolean hasSameNameBroken) {
            return new ChangedItem(type, schemaNode, null, null, null,
                    new BaseItemParameter(hasSameName, hasSameNameBroken, true, true, false));
        }

        public String getModelAlias() {
            return modelAlias != null ? modelAlias : schemaNode.getSubject();
        }

        public String getDetail() {
            return getDetail(schemaNode);
        }

        public Map<String, Object> getAttributes() {
            return schemaNode.getAttributes();
        }
    }

    @Data
    public static class UpdatedItem extends BaseItem {
        @JsonIgnore
        private SchemaNode firstSchemaNode;

        @JsonIgnore
        private SchemaNode secondSchemaNode;

        @JsonProperty("first_attributes")
        public Map<String, Object> getFirstAttributes() {
            return firstSchemaNode.getAttributes();
        }

        @JsonProperty("second_attributes")
        public Map<String, Object> getSecondAttributes() {
            return secondSchemaNode.getAttributes();
        }

        @JsonProperty("first_detail")
        public String getFirstDetail() {
            return getDetail(firstSchemaNode);
        }

        @JsonProperty("second_detail")
        public String getSecondDetail() {
            return getDetail(secondSchemaNode);
        }

        public UpdatedItem(SchemaNode first, SchemaNode second, String modelAlias, UN_IMPORT_REASON reason,
                String conflictItem, BaseItemParameter parameter) {
            super(second.getType(), modelAlias, new ConflictReason(reason, conflictItem), parameter.hasSameName,
                    parameter.hasSameNameBroken, parameter.importable, parameter.creatable, parameter.overwritable);
            this.firstSchemaNode = first;
            this.secondSchemaNode = second;
        }

        public static UpdatedItem getSchemaUpdate(SchemaNode first, SchemaNode second, String modelAlias,
                UN_IMPORT_REASON reason, String conflictItem, BaseItemParameter parameter) {
            return new UpdatedItem(first, second, modelAlias, reason, conflictItem, parameter);
        }

        public static UpdatedItem getSchemaUpdate(SchemaNode first, SchemaNode second, String modelAlias,
                BaseItemParameter parameter) {
            return getSchemaUpdate(first, second, modelAlias, UN_IMPORT_REASON.NONE, null, parameter);
        }
    }

    public void addMissingItems(List<ChangedItem> missingItems) {
        missingItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getMissingItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    public void addNewItems(List<ChangedItem> newItems) {
        newItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getNewItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    public void addUpdateItems(List<UpdatedItem> updateItems) {
        updateItems.forEach(item -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(item.getModelAlias(), new ModelSchemaChange());
            modelSchemaChange.getUpdateItems().add(item);
            models.put(item.getModelAlias(), modelSchemaChange);
        });
    }

    public void addReduceItems(List<ChangedItem> reduceItems) {
        reduceItems.forEach(schemaChange -> {
            ModelSchemaChange modelSchemaChange = models.getOrDefault(schemaChange.getModelAlias(),
                    new ModelSchemaChange());
            modelSchemaChange.getReduceItems().add(schemaChange);
            models.put(schemaChange.getModelAlias(), modelSchemaChange);
        });
    }

    @JsonIgnore
    public void areEqual(List<String> modelAlias) {
        modelAlias.forEach(model -> models.putIfAbsent(model, new ModelSchemaChange()));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConflictReason {
        @JsonProperty("reason")
        UN_IMPORT_REASON reason;

        @JsonProperty("conflict_item")
        String conflictItem;
    }

    public enum UN_IMPORT_REASON {
        SAME_CC_NAME_HAS_DIFFERENT_EXPR, //
        DIFFERENT_CC_NAME_HAS_SAME_EXPR, //
        USED_UNLOADED_TABLE, //
        TABLE_COLUMN_DATATYPE_CHANGED, //
        MISSING_TABLE_COLUMN, //
        MISSING_TABLE, //
        NONE;
    }

    @Data
    @AllArgsConstructor
    public static class BaseItemParameter {
        private boolean hasSameName;
        private boolean hasSameNameBroken;
        private boolean importable;
        private boolean creatable;
        private boolean overwritable;
    }
}
