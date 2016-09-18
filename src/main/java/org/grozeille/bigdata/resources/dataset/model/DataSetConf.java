package org.grozeille.bigdata.resources.dataset.model;

import lombok.Data;

@Data
public class DataSetConf {

    private String id;

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format = "view";

    private DataSetConfTable[] tables;

    private DataSetConfLink[] links;

    private DataSetConfColumn[] calculatedColumns;

    private DataSetFilterGroup filter;
}
