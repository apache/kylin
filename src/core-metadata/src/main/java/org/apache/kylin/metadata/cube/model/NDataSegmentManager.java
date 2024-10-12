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

package org.apache.kylin.metadata.cube.model;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.metadata.Manager;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NDataSegmentManager extends Manager<NDataSegment> {
    private static final Logger logger = LoggerFactory.getLogger(NDataSegmentManager.class);

    protected NDataSegmentManager(KylinConfig cfg, String project, MetadataType type) {
        super(cfg, project, type);
    }

    // called by reflection
    static NDataSegmentManager newInstance(KylinConfig config, String project) {
        return new NDataSegmentManager(config, project, MetadataType.SEGMENT);
    }

    public static NDataSegmentManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, NDataSegmentManager.class);
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public String name() {
        return "NDataSegmentManager";
    }

    @Override
    public Class<NDataSegment> entityType() {
        return NDataSegment.class;
    }

    @Override
    protected void initCrud(MetadataType type, String project) {
        this.crud = new CachedCrudAssist<NDataSegment>(getStore(), type, project, entityType()) {
            @Override
            protected NDataSegment initEntityAfterReload(NDataSegment entity, String resourceName) {
                entity.initAfterReload();
                return entity;
            }
        };
        this.crud.setCheckCopyOnWrite(true);
    }

    @Override
    public NDataSegment copyForWrite(NDataSegment entity) {
        NDataSegment copy = super.copyForWrite(entity);
        if (entity.getDataflow() != null) {
            copy.setDataflow(entity.getDataflow());
        }
        return copy;
    }

    @Override
    public NDataSegment copy(NDataSegment entity) {
        NDataSegment copy = super.copy(entity);
        if (entity.getDataflow() != null) {
            copy.setDataflow(entity.getDataflow());
        }
        return copy;
    }

    @Override
    public Optional<NDataSegment> get(String resourceName) {
        Optional<NDataSegment> segment = getWithoutInitDataflow(resourceName);
        segment.ifPresent(NDataSegment::initDataFlow);
        return segment;
    }

    public Optional<NDataSegment> getWithoutInitDataflow(String resourceName) {
        if (StringUtils.isEmpty(resourceName)) {
            return Optional.empty();
        }
        return Optional.ofNullable(crud.get(resourceName));
    }
    
    public Segments<NDataSegment> getSegmentsUnderDataflow(NDataflow df) {
        return getSegments(df, df.getSegmentUuids());
    }

    public Segments<NDataSegment> getSegments(NDataflow df, Collection<String> segIds) {
        List<NDataSegment> segments = segIds.stream().map(uuid -> getWithoutInitDataflow(uuid).orElse(null))
                .filter(Objects::nonNull).collect(Collectors.toList());
        segments.forEach(seg -> seg.setDataflow(df));
        return new Segments<>(segments);
    }
}
