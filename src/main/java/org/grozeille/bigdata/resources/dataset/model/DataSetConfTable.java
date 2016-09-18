package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetConfTable {

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format;

    private Boolean primary;

    private DataSetConfColumn[] columns;
}
