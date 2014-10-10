package com.kylinolap.query.sqlfunc;

import net.hydromatic.optiq.runtime.SqlFunctions;
import net.hydromatic.optiq.runtime.SqlFunctions.TimeUnitRange;

public abstract class QuarterBase {
    
    public static long eval(int date) {
        long month = SqlFunctions.unixDateExtract(TimeUnitRange.MONTH, date);
        return month / 4 + 1;
    }
}
