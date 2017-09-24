package fr.grozeille.scuba.dataset.web.dto;

import fr.grozeille.scuba.dataset.model.CustomFileDataSetConf;
import lombok.Data;

@Data
public class CustomFileDataSetRequestConfig {

    private CustomFileDataSetConf.CustomFileDataSetFormat fileFormat;

    private String sheet;

    private Character separator;

    private Character textQualifier;

    private boolean firstLineHeader;
}
