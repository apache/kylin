package org.apache.kylin.rest.request;

/**
 * Created by honma on 8/7/14.
 */
public class UpdateProjectRequest {
    private String formerProjectName;
    private String newProjectName;
    private String newDescription;

    public UpdateProjectRequest() {
    }

    public String getFormerProjectName() {
        return formerProjectName;
    }

    public void setFormerProjectName(String formerProjectName) {

        this.formerProjectName = formerProjectName;
    }

    public String getNewDescription() {
        return newDescription;
    }

    public void setNewDescription(String newDescription) {
        this.newDescription = newDescription;
    }

    public String getNewProjectName() {
        return newProjectName;
    }

    public void setNewProjectName(String newProjectName) {
        this.newProjectName = newProjectName;
    }
}
