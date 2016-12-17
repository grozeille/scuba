package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

@Data
public class UserDataSetConf {

    private String id;

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format = "view";

    private UserDataSetConfTable[] tables;

    private UserDataSetConfLink[] links;

    private UserDataSetConfColumn[] calculatedColumns;

    private UserDataSetFilterGroup filter;
}
