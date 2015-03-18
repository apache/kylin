package org.apache.kylin.streaming.nous;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * Created by Hongbin Ma(Binmahone) on 3/16/15.
 *
 * The kafka message format for Nous
 */
@JsonAutoDetect
public class NousMessage {
    private long minute_start;
    private long hour_start;
    private String itm;
    private String t;
    private String sid;
    private String p;
    private String m;
    private long click;
    private double gmv;
    private long qty;

    public NousMessage() {
    }

    public NousMessage(long minute_start, long hour_start, String itm, String t, String sid, String p, String m, long click, double gmv, long qty) {
        this.minute_start = minute_start;
        this.hour_start = hour_start;
        this.itm = itm;
        this.t = t;
        this.sid = sid;
        this.p = p;
        this.m = m;
        this.click = click;
        this.gmv = gmv;
        this.qty = qty;
    }

    public long getMinute_start() {
        return minute_start;
    }

    public void setMinute_start(long minute_start) {
        this.minute_start = minute_start;
    }

    public long getHour_start() {
        return hour_start;
    }

    public void setHour_start(long hour_start) {
        this.hour_start = hour_start;
    }

    public String getItm() {
        return itm;
    }

    public void setItm(String itm) {
        this.itm = itm;
    }

    public String getT() {
        return t;
    }

    public void setT(String t) {
        this.t = t;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    public String getP() {
        return p;
    }

    public void setP(String p) {
        this.p = p;
    }

    public String getM() {
        return m;
    }

    public void setM(String m) {
        this.m = m;
    }

    public long getClick() {
        return click;
    }

    public void setClick(long click) {
        this.click = click;
    }

    public double getGmv() {
        return gmv;
    }

    public void setGmv(double gmv) {
        this.gmv = gmv;
    }

    public long getQty() {
        return qty;
    }

    public void setQty(long qty) {
        this.qty = qty;
    }
}
