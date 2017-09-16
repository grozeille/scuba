package org.grozeille.bigdata.dataset.web.dto;

import lombok.Data;
import org.grozeille.bigdata.dataset.model.CustomFileDataSetConf;

@Data
public class CustomFileDataSetRequest {
    private String comment;

    private String[] tags;

    private Boolean temporary;

    private CustomFileDataSetRequestConfig dataSetConfig;
}
