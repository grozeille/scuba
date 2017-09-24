package fr.grozeille.scuba.project.web.dto;

import lombok.Data;

@Data
public class NewProjectRequest {
    String name;
    String hiveDatabase;
    String hdfsWorkingDirectory;
}
