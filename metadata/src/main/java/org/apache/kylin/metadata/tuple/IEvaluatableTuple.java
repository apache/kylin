package org.apache.kylin.metadata.tuple;

import org.apache.kylin.metadata.model.TblColRef;

public interface IEvaluatableTuple {

    public Object getValue(TblColRef col);

}
