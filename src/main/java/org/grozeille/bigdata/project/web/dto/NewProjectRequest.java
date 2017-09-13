package org.grozeille.bigdata.project.web.dto;

import lombok.Data;

@Data
public class NewProjectRequest {
    String name;
    String hiveDatabase;
    String hdfsWorkingDirectory;
}
