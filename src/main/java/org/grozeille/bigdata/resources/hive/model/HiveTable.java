package org.grozeille.bigdata.resources.hive.model;

import lombok.Data;

@Data
public class HiveTable {
    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format;

    private String datalakeItemType;

    private String originalFile;

    private String datsetConfiguration;

    private HiveColumn[] columns;
}
