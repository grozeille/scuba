package org.grozeille.bigdata.resources.project.model;

import lombok.Data;

@Data
public class NewProjectRequest {
    String name;
    String hiveDatabase;
    String hdfsWorkingDirectory;
}
