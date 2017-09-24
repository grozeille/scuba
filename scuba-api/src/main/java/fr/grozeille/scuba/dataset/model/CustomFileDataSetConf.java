package fr.grozeille.scuba.dataset.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomFileDataSetConf {

    public enum CustomFileDataSetFormat {
        RAW, CSV, EXCEL
    }

    private CustomFileDataSetFormat fileFormat;

    private String sheet;

    private Character separator;

    private Character textQualifier;

    private boolean firstLineHeader;

    private CustomFileDataSetFileInfo originalFile;
}
