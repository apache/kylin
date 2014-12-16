package com.kylinolap.job2.dao;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.kylinolap.common.persistence.RootPersistentEntity;

import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class JobPO extends RootPersistentEntity {

    private String name;

    private long startTime;

    private long endTime;

    private String status;

    private List<JobPO> tasks;
}
