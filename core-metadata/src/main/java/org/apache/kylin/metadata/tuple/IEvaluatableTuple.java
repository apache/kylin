package org.apache.kylin.metadata.tuple;

import org.apache.kylin.metadata.model.TblColRef;

public interface IEvaluatableTuple {

    Object getValue(TblColRef col);

}
