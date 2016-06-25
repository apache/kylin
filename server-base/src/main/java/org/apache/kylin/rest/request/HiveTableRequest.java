package org.apache.kylin.rest.request;

/**
 * Created by kangkaisen on 16/5/21.
 */
public class HiveTableRequest {

    public HiveTableRequest() {

    }

    private boolean calculate = true;

    public boolean isCalculate() {
        return calculate;
    }

    public void setCalculate(boolean calculate) {
        this.calculate = calculate;
    }

}
