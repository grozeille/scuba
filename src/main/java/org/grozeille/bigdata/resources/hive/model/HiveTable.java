package org.grozeille.bigdata.resources.hive.model;

import lombok.Data;

@Data
public class HiveTable {
    private String database;

    private String table;

    private String path;

    private String comment;

    private String creator;

    private String[] tags;

    private String format;

    private String datalakeItemType;

    private String originalFile;

    private String originalFileContentType;

    private Integer originalFileSize;

    private String datsetConfiguration;

    private Boolean temporary;

    private HiveColumn[] columns;
}
