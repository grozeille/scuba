package org.grozeille.bigdata.resources.customfiledataset.model;

import lombok.Data;
import org.springframework.web.bind.annotation.RequestParam;

@Data
public class CustomFileDataSetFormatRequest {
    public enum CustomFileDataSetFormat {
        RAW, CSV, EXCEL
    }
    private CustomFileDataSetFormat format;
    private String sheet;
    private String separator;
    private String textQualifier;
    private boolean firstLineHeader;
}
