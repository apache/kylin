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
package io.kyligence.kap.secondstorage.metadata;

import org.apache.kylin.metadata.cube.model.LayoutEntity;

import java.util.List;
import java.util.Optional;

public interface HasLayoutElement<E extends WithLayout> {

    // utility
    static <E extends WithLayout> boolean sameLayout(E e, LayoutEntity layoutEntity){
        return sameLayout(e, layoutEntity.getId());
    }

    static <E extends WithLayout> boolean sameLayout(E e, long layoutId){
        return e.getLayoutID() == layoutId;
    }

    static <E extends WithLayout> boolean sameIndex(E e, LayoutEntity layoutEntity){
        return layoutEntity.getIndexId() == e.getLayoutID() / 10000 * 10000;
    }

    List<E> all();

    default Optional<E> getEntity(LayoutEntity layoutEntity, boolean sameLayout){
        return all().stream()
                .filter(e -> sameLayout ? sameLayout(e, layoutEntity): sameIndex(e, layoutEntity))
                .findFirst();
    }

    default Optional<E> getEntity(LayoutEntity layoutEntity) {
        return getEntity(layoutEntity.getId());
    }

    default Optional<E> getEntity(long layout) {
        return all().stream()
                .filter(e -> sameLayout(e, layout))
                .findFirst();
    }

    default boolean containIndex(LayoutEntity layoutEntity, boolean throwOnDifferentLayout){
        return getEntity(layoutEntity, false)
                .map(e -> {
                            if (!throwOnDifferentLayout || sameLayout(e, layoutEntity))
                                return true;
                            else
                                throw new IllegalStateException("");
                        })
                .orElse(false);
    }
}
