package org.grozeille.bigdata.resources.customfiledataset.model;

import lombok.Data;

@Data
public class PreviewCustomFileDataSetFormatRequest {
    private static final Long MAX_LINES_PREVIEW = 5000l;

    public enum CustomFileDataSetFormat {
        RAW, CSV, EXCEL
    }
    private CustomFileDataSetFormat format;
    private String sheet;
    private Character separator;
    private Character textQualifier;
    private boolean firstLineHeader;

    private Long maxLinePreview = MAX_LINES_PREVIEW;
}
