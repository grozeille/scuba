package fr.grozeille.scuba.dataset.web.dto;

import fr.grozeille.scuba.dataset.model.WranglingDataSetConf;
import lombok.Data;

@Data
public class WranglingDataSetRequest {
    private String comment;

    private String[] tags;

    private Boolean temporary;

    private WranglingDataSetConf dataSetConfig;
}
