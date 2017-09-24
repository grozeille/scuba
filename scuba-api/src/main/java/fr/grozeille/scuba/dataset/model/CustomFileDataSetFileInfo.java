package fr.grozeille.scuba.dataset.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomFileDataSetFileInfo {
    private String path;

    private Integer size;

    private String contentType;
}
