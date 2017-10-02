package fr.grozeille.scuba.dataset.web.dto;

import lombok.Data;

@Data
public class DataSetRequest {
    private String comment;

    private String[] tags;
}
