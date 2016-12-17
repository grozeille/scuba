package org.grozeille.bigdata.resources.userdataset.model;

import lombok.Data;

@Data
public class UserDataSetConfTable {

    private String database;

    private String table;

    private String path;

    private String comment;

    private String dataDomainOwner;

    private String[] tags;

    private String format;

    private Boolean primary;

    private UserDataSetConfColumn[] columns;
}
