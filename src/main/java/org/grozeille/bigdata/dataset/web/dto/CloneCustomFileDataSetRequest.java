package org.grozeille.bigdata.dataset.web.dto;

import lombok.Data;

@Data
public class CloneCustomFileDataSetRequest {
    private String targetDatabase;

    private String targetTable;

    private Boolean temporary;
}
