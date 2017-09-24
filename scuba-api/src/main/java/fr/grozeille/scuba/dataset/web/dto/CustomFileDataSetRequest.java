package fr.grozeille.scuba.dataset.web.dto;

import lombok.Data;

@Data
public class CustomFileDataSetRequest {
    private String comment;

    private String[] tags;

    private Boolean temporary;

    private CustomFileDataSetRequestConfig dataSetConfig;
}
