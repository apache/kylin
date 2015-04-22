package org.apache.kylin.rest.model;

/**
 * Created by jiazhong on 2015/4/13.
 */
public class Performance {

    public int query_total;

    public double query_qps;

    public double query_latency;

    public int job_total;

    public double job_average;

    public int job_mr_waiting;

    public int cube_total;

    public int cube_active;

    public double cube_storage;

    public int source_total;

    public int source_records;

    public double source_storage;

    public int getQuery_total() {
        return query_total;
    }

    public void setQuery_total(int query_total) {
        this.query_total = query_total;
    }

    public double getQuery_qps() {
        return query_qps;
    }

    public void setQuery_qps(double query_qps) {
        this.query_qps = query_qps;
    }

    public double getQuery_latency() {
        return query_latency;
    }

    public void setQuery_latency(double query_latency) {
        this.query_latency = query_latency;
    }

    public int getJob_total() {
        return job_total;
    }

    public void setJob_total(int job_total) {
        this.job_total = job_total;
    }

    public double getJob_average() {
        return job_average;
    }

    public void setJob_average(double job_average) {
        this.job_average = job_average;
    }

    public int getJob_mr_waiting() {
        return job_mr_waiting;
    }

    public void setJob_mr_waiting(int job_mr_waiting) {
        this.job_mr_waiting = job_mr_waiting;
    }

    public int getCube_total() {
        return cube_total;
    }

    public void setCube_total(int cube_total) {
        this.cube_total = cube_total;
    }

    public int getCube_active() {
        return cube_active;
    }

    public void setCube_active(int cube_active) {
        this.cube_active = cube_active;
    }

    public double getCube_storage() {
        return cube_storage;
    }

    public void setCube_storage(double cube_storage) {
        this.cube_storage = cube_storage;
    }

    public int getSource_total() {
        return source_total;
    }

    public void setSource_total(int source_total) {
        this.source_total = source_total;
    }

    public int getSource_records() {
        return source_records;
    }

    public void setSource_records(int source_records) {
        this.source_records = source_records;
    }

    public double getSource_storage() {
        return source_storage;
    }

    public void setSource_storage(double source_storage) {
        this.source_storage = source_storage;
    }
}
