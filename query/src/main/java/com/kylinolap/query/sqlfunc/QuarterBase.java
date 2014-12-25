package com.kylinolap.query.sqlfunc;

import net.hydromatic.optiq.runtime.SqlFunctions;
import net.hydromatic.optiq.runtime.SqlFunctions.TimeUnitRange;

/**
 * @author xjiang
 * 
 */
public abstract class QuarterBase {

    /**
     * According to jvm spec, it return self method before parent.
     * So, we keep Date in parent and int in child
     */
    public static long eval(int date) {
        long month = SqlFunctions.unixDateExtract(TimeUnitRange.MONTH, date);
        return month / 4 + 1;
    }
}
