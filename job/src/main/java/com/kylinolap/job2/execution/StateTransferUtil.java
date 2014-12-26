package com.kylinolap.job2.execution;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Set;

/**
 * Created by qianzhou on 12/26/14.
 */
public final class StateTransferUtil {

    private StateTransferUtil() {}

    private static Multimap<ExecutableStatus, ExecutableStatus> VALID_STATE_TRANSFER;

    static {
        VALID_STATE_TRANSFER = Multimaps.newSetMultimap(Maps.<ExecutableStatus, Collection<ExecutableStatus>>newEnumMap(ExecutableStatus.class), new Supplier<Set<ExecutableStatus>>() {
            @Override
            public Set<ExecutableStatus> get() {
                return Sets.newCopyOnWriteArraySet();
            }
        });
        VALID_STATE_TRANSFER.put(ExecutableStatus.READY, ExecutableStatus.RUNNING);

        VALID_STATE_TRANSFER.put(ExecutableStatus.RUNNING, ExecutableStatus.READY);
        VALID_STATE_TRANSFER.put(ExecutableStatus.RUNNING, ExecutableStatus.SUCCEED);
        VALID_STATE_TRANSFER.put(ExecutableStatus.RUNNING, ExecutableStatus.STOPPED);
        VALID_STATE_TRANSFER.put(ExecutableStatus.RUNNING, ExecutableStatus.ERROR);

        VALID_STATE_TRANSFER.put(ExecutableStatus.ERROR, ExecutableStatus.READY);

        VALID_STATE_TRANSFER.put(ExecutableStatus.STOPPED, ExecutableStatus.DISCARDED);
        VALID_STATE_TRANSFER.put(ExecutableStatus.STOPPED, ExecutableStatus.READY);
    }

    public static boolean isValidStateTransfer(ExecutableStatus from, ExecutableStatus to) {
        return VALID_STATE_TRANSFER.containsEntry(from, to);
    }

}
