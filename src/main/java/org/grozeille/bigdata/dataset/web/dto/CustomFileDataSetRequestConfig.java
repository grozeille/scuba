package org.grozeille.bigdata.dataset.web.dto;

import lombok.Data;
import org.grozeille.bigdata.dataset.model.CustomFileDataSetConf;

@Data
public class CustomFileDataSetRequestConfig {

    private CustomFileDataSetConf.CustomFileDataSetFormat fileFormat;

    private String sheet;

    private Character separator;

    private Character textQualifier;

    private boolean firstLineHeader;
}
