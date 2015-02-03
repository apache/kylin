package org.apache.kylin.rest.request;

/**
 * Created by honma on 8/7/14.
 */
public class CreateProjectRequest {
    private String name;
    private String description;

    public CreateProjectRequest() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
