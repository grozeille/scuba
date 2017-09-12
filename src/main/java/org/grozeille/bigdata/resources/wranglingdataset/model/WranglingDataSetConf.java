package org.grozeille.bigdata.resources.wranglingdataset.model;

import lombok.Data;

@Data
public class WranglingDataSetConf {

    private String id;

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format = "view";

    private WranglingDataSetConfTable[] tables;

    private WranglingDataSetConfLink[] links;

    private WranglingDataSetConfColumn[] calculatedColumns;

    private WranglingDataSetFilterGroup filter;
}
