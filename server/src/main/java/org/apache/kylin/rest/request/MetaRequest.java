package org.apache.kylin.rest.request;

/**
 * Created by honma on 7/10/14.
 */
public class MetaRequest {

    private String project;

    public MetaRequest() {
    }

    public MetaRequest(String project) {
        this.project = project;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }
}
