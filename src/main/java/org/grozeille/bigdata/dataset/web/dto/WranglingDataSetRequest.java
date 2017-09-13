package org.grozeille.bigdata.dataset.web.dto;

import lombok.Data;
import org.grozeille.bigdata.dataset.model.WranglingDataSetConf;

@Data
public class WranglingDataSetRequest {
    private String comment;

    private String[] tags;

    private Boolean temporary;

    private WranglingDataSetConf dataSetConfig;
}
